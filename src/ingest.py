#!/usr/bin/env python3
"""
Beginner-friendly NBA ingest script (no database).
- Fetches today's and yesterday's games from NBA public JSON endpoints.
- Writes/updates export/games.csv and export/player_stats.csv.
- Idempotent: dedupes by (gameId) for games and (gameId, playerId) for players.

Notes:
- Uses the NBA "liveData" CDN endpoints.
- If these endpoints change, see the "Adjust parsing here" comments.
"""

from __future__ import annotations
import csv
import os
import sys
import time
from typing import Dict, Any, List
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo  # Python 3.9+
import requests
import pandas as pd

# Use a browser-like User-Agent to avoid occasional 403s
HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"
}

ET = ZoneInfo("America/New_York")
EXPORT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "export"))
os.makedirs(EXPORT_DIR, exist_ok=True)

GAMES_CSV = os.path.join(EXPORT_DIR, "games.csv")
PLAYERS_CSV = os.path.join(EXPORT_DIR, "player_stats.csv")

USE_PARQUET = os.environ.get("USE_PARQUET", "0") == "1"

# ---------- Helpers ----------

def et_now_date():
    return datetime.now(tz=ET).date()

def date_yyyymmdd(d):
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"

def get_scoreboard_for_date(d) -> Dict[str, Any]:
    """
    Try date-specific scoreboard first; if it's today, also try today's generic endpoint.
    Return {} on failure.
    """
    ymd = date_yyyymmdd(d)
    candidates = [
        f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json",
    ]
    if d == et_now_date():
        candidates.append("https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json")

    for url in candidates:
        try:
            r = requests.get(url, timeout=20, headers=HEADERS)
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print(f"[warn] scoreboard fetch failed for {url}: {e}", file=sys.stderr)
    return {}

def get_boxscore_for_game(game_id: str) -> Dict[str, Any]:
    """
    Boxscore endpoint per game ID.
    """
    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[warn] boxscore fetch failed for {game_id}: {e}", file=sys.stderr)
    return {}

def ensure_csv(path: str, columns: List[str]):
    """
    Create CSV with header if it doesn't exist.
    """
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(columns)

def upsert_csv(path: str, df_new: pd.DataFrame, dedupe_keys: List[str]):
    """
    Append new rows to CSV, then dedupe by given keys, keeping the last occurrence.
    """
    if os.path.exists(path):
        df_old = pd.read_csv(path)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()

    df_all.drop_duplicates(subset=dedupe_keys, keep="last", inplace=True)
    df_all.to_csv(path, index=False)

    if USE_PARQUET:
        try:
            pq_path = os.path.splitext(path)[0] + ".parquet"
            df_all.to_parquet(pq_path, index=False)
        except Exception as e:
            print(f"[warn] parquet write skipped: {e}", file=sys.stderr)

# ---------- Main ingest ----------

def collect_from_scoreboard(sb_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parses scoreboard JSON into a list of per-game dicts.
    Adjust parsing here if NBA changes field names.
    """
    games = []
    if not sb_json:
        return games

    # Typical structure: {"scoreboard": {"games": [ ... ]}}
    scoreboard = sb_json.get("scoreboard", {})
    for g in scoreboard.get("games", []):
        game = {
            "gameId": g.get("gameId"),
            "gameCode": g.get("gameCode"),
            "gameDateEt": g.get("gameEt"),   # e.g., "2025-01-12T19:30:00-05:00"
            "gameStatusText": g.get("gameStatusText"),
            "period": g.get("period"),
            "gameClock": g.get("gameClock"),
            "arenaName": (g.get("arenaName") or ""),
        }

        home = g.get("homeTeam", {}) or {}
        away = g.get("awayTeam", {}) or {}

        game.update({
            "homeTeamId": home.get("teamId"),
            "homeTeamTricode": home.get("teamTricode"),
            "homeScore": home.get("score"),
            "awayTeamId": away.get("teamId"),
            "awayTeamTricode": away.get("teamTricode"),
            "awayScore": away.get("score"),
        })
        if not game["gameId"]:
            continue
        games.append(game)
    return games

def collect_players_from_boxscore(bs_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parses boxscore JSON into per-player rows.
    Adjust parsing here if NBA changes field names.
    """
    rows = []
    if not bs_json:
        return rows

    game = bs_json.get("game", {})
    box = game.get("boxScore", {})
    players = box.get("players", []) or []
    game_id = game.get("gameId")

    teams = {t.get("teamTricode"): t for t in box.get("teams", [])} if isinstance(box.get("teams", []), list) else {}

    for p in players:
        team_tricode = p.get("teamTricode")
        teamId = teams.get(team_tricode, {}).get("teamId") if team_tricode in teams else None

        stats = p.get("statistics", {}) or {}
        row = {
            "gameId": game_id,
            "playerId": p.get("personId"),
            "teamId": teamId,
            "teamTricode": team_tricode,
            "firstName": p.get("firstName"),
            "familyName": p.get("familyName"),
            "jerseyNum": p.get("jerseyNum"),
            "position": p.get("position"),
            "minutes": stats.get("minutes"),
            "points": stats.get("points"),
            "reboundsTotal": stats.get("reboundsTotal"),
            "assists": stats.get("assists"),
            "steals": stats.get("steals"),
            "blocks": stats.get("blocks"),
            "turnovers": stats.get("turnovers"),
            "fieldGoalsMade": stats.get("fieldGoalsMade"),
            "fieldGoalsAttempted": stats.get("fieldGoalsAttempted"),
            "threePointersMade": stats.get("threePointersMade"),
            "threePointersAttempted": stats.get("threePointersAttempted"),
            "freeThrowsMade": stats.get("freeThrowsMade"),
            "freeThrowsAttempted": stats.get("freeThrowsAttempted"),
            "plusMinus": stats.get("plusMinusPoints"),
            "didNotPlay": p.get("didNotPlay"),
            "notPlayingReason": p.get("notPlayingReason"),
        }
        if row["gameId"] and row["playerId"]:
            rows.append(row)

    return rows

def main():
    today = et_now_date()
    yesterday = today - timedelta(days=1)

    print(f"[info] collecting games for {yesterday} and {today} (ET)")

    all_games: List[Dict[str, Any]] = []
    for d in [yesterday, today]:
        sb = get_scoreboard_for_date(d)
        games = collect_from_scoreboard(sb)
        all_games.extend(games)

    print(f"[info] total games discovered: {len(all_games)}")

    # Persist games
    if all_games:
        games_df = pd.DataFrame(all_games)
        ensure_csv(GAMES_CSV, list(games_df.columns))
        upsert_csv(GAMES_CSV, games_df, dedupe_keys=["gameId"])
    else:
        print("[warn] no games found for the target dates")

    # Collect players per game
    all_players: List[Dict[str, Any]] = []
    for g in all_games:
        gid = g.get("gameId")
        if not gid:
            continue
        bs = get_boxscore_for_game(gid)
        rows = collect_players_from_boxscore(bs)
        print(f"[info] game {gid}: {len(rows)} player rows")
        all_players.extend(rows)
        time.sleep(0.4)  # gentle pacing

    if all_players:
        players_df = pd.DataFrame(all_players)
        ensure_csv(PLAYERS_CSV, list(players_df.columns))
        upsert_csv(PLAYERS_CSV, players_df, dedupe_keys=["gameId", "playerId"])
    else:
        print("[warn] no player rows collected")

    print("[done] export complete")

if __name__ == "__main__":
    main()
