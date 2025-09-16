#!/usr/bin/env python3
"""
Beginner-friendly NBA ingest script (no database).
- Fetches NBA games/boxscores from public JSON endpoints.
- Writes/updates export/games.csv, export/team_stats.csv, export/player_stats.csv.
- Idempotent: dedupes by (gameId) for games, (gameId, teamId) for team totals,
  and (gameId, playerId) for players.

Date control (optional, via env):
- START_DATE / END_DATE (YYYY-MM-DD) -> inclusive backfill range
- or DAYS_BACK=N                     -> today back to N days ago
- default (no env): yesterday + today (ET)
"""

from __future__ import annotations
import csv
import os
import sys
import time
from typing import Dict, Any, List
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import requests
import pandas as pd

# Browser-like UA avoids rare 403s from the CDN
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari"}

ET = ZoneInfo("America/New_York")
EXPORT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "export"))
os.makedirs(EXPORT_DIR, exist_ok=True)

GAMES_CSV   = os.path.join(EXPORT_DIR, "games.csv")
TEAM_CSV    = os.path.join(EXPORT_DIR, "team_stats.csv")
PLAYERS_CSV = os.path.join(EXPORT_DIR, "player_stats.csv")

USE_PARQUET = os.environ.get("USE_PARQUET", "0") == "1"

# ---------- helpers ----------

def et_now_date() -> date:
    return datetime.now(tz=ET).date()

def date_yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"

def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d = d + timedelta(days=1)

def get_scoreboard_for_date(d: date) -> Dict[str, Any]:
    """Try date-specific scoreboard first; if today, also try 'todaysScoreboard_00'."""
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
    """Per-game boxscore."""
    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    try:
        r = requests.get(url, timeout=20, headers=HEADERS)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[warn] boxscore fetch failed for {game_id}: {e}", file=sys.stderr)
    return {}

def ensure_csv(path: str, columns: List[str]):
    """Create CSV with header if it doesn't exist."""
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(columns)

def upsert_csv(path: str, df_new: pd.DataFrame, dedupe_keys: List[str]):
    """Append and dedupe by keys (keep last). Also write optional parquet mirror."""
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

# ---------- parsing ----------

def collect_from_scoreboard(sb_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert scoreboard JSON into per-game dicts."""
    games = []
    if not sb_json:
        return games

    scoreboard = sb_json.get("scoreboard", {}) or {}
    for g in scoreboard.get("games", []) or []:
        home = g.get("homeTeam", {}) or {}
        away = g.get("awayTeam", {}) or {}
        game = {
            "gameId": g.get("gameId"),
            "gameCode": g.get("gameCode"),
            "gameDateEt": g.get("gameEt"),
            "gameStatusText": g.get("gameStatusText"),
            "period": g.get("period"),
            "gameClock": g.get("gameClock"),
            "arenaName": (g.get("arenaName") or ""),
            "homeTeamId": home.get("teamId"),
            "homeTeamTricode": home.get("teamTricode"),
            "homeScore": home.get("score"),
            "awayTeamId": away.get("teamId"),
            "awayTeamTricode": away.get("teamTricode"),
            "awayScore": away.get("score"),
        }
        if game["gameId"]:
            games.append(game)
    return games

def collect_players_from_boxscore(bs_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert boxscore JSON into per-player rows."""
    rows = []
    if not bs_json:
        return rows

    game = bs_json.get("game", {}) or {}
    box  = game.get("boxScore", {}) or {}
    players = box.get("players", []) or []
    game_id = game.get("gameId")

    teams = {t.get("teamTricode"): t for t in (box.get("teams", []) or [])}

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

def collect_teamstats_from_boxscore(bs_json: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert boxscore JSON into per-team total rows."""
    rows = []
    if not bs_json:
        return rows

    game = bs_json.get("game", {}) or {}
    box  = game.get("boxScore", {}) or {}

    teams_list = box.get("teams", []) or []
    game_id = game.get("gameId")

    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}
    home_id, away_id   = home.get("teamId"), away.get("teamId")
    home_score, away_score = (home.get("score") or 0), (away.get("score") or 0)

    id_to_score = {home_id: home_score, away_id: away_score}
    id_to_opp   = {home_id: away_id, away_id: home_id}

    for t in teams_list:
        team_id = t.get("teamId")
        tri     = t.get("teamTricode")
        city    = t.get("teamCity")
        name    = t.get("teamName")
        score   = t.get("score")
        stats   = t.get("statistics", {}) or {}
        pts     = score if score is not None else stats.get("points")

        is_home = 1 if team_id == home_id else 0 if team_id == away_id else None
        opp_id  = id_to_opp.get(team_id)
        win     = None
        if team_id in id_to_score and opp_id in id_to_score:
            try:
                win = 1 if int(id_to_score[team_id]) > int(id_to_score[opp_id]) else 0
            except Exception:
                pass

        row = {
            "gameId": game_id,
            "teamId": team_id,
            "teamTricode": tri,
            "teamCity": city,
            "teamName": name,
            "opponentTeamId": opp_id,
            "home": is_home,   # 1=home, 0=away
            "win": win,        # 1/0 when determinable
            "points": pts,
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
        }
        if row["gameId"] and row["teamId"]:
            rows.append(row)

    return rows

# ---------- main ----------

def main():
    # pick date targets
    start_env = os.environ.get("START_DATE")  # YYYY-MM-DD
    end_env   = os.environ.get("END_DATE")    # YYYY-MM-DD
    days_back = os.environ.get("DAYS_BACK")   # integer

    def parse_ymd(s: str) -> date:
        return datetime.strptime(s, "%Y-%m-%d").date()

    today = et_now_date()
    targets: List[date] = []

    if start_env and end_env:
        start_d, end_d = parse_ymd(start_env), parse_ymd(end_env)
        if end_d < start_d:
            start_d, end_d = end_d, start_d
        targets = list(daterange(start_d, end_d))
        print(f"[info] backfill range {start_d}..{end_d} ({len(targets)} days)")
    elif days_back:
        n = max(1, int(days_back))
        targets = [today - timedelta(days=i) for i in range(n, -1, -1)]
        print(f"[info] last {n} days through {today}")
    else:
        targets = [today - timedelta(days=1), today]
        print(f"[info] window: {targets[0]} & {targets[1]} (ET)")

    # collect games for all target dates
    all_games: List[Dict[str, Any]] = []
    for d in targets:
        sb = get_scoreboard_for_date(d)
        games = collect_from_scoreboard(sb)
        for gg in games:
            print(f"[game] {gg.get('gameId')} {gg.get('awayTeamTricode')}@{gg.get('homeTeamTricode')} {gg.get('gameStatusText')}")
        all_games.extend(games)

    print(f"[info] total games discovered: {len(all_games)}")

    if all_games:
        games_df = pd.DataFrame(all_games)
        ensure_csv(GAMES_CSV, list(games_df.columns))
        upsert_csv(GAMES_CSV, games_df, dedupe_keys=["gameId"])
    else:
        print("[warn] no games found for target dates")

    # per-game players & teams
    all_players: List[Dict[str, Any]] = []
    all_teamstats: List[Dict[str, Any]] = []
    for g in all_games:
        gid = g.get("gameId")
        if not gid:
            continue
        bs = get_boxscore_for_game(gid)
        prows = collect_players_from_boxscore(bs)
        trows = collect_teamstats_from_boxscore(bs)
        print(f"[info] game {gid}: {len(prows)} player rows, {len(trows)} team rows")
        all_players.extend(prows)
        all_teamstats.extend(trows)
        time.sleep(0.4)

    if all_players:
        players_df = pd.DataFrame(all_players)
        ensure_csv(PLAYERS_CSV, list(players_df.columns))
        upsert_csv(PLAYERS_CSV, players_df, dedupe_keys=["gameId", "playerId"])
    else:
        print("[warn] no player rows collected")

    if all_teamstats:
        team_df = pd.DataFrame(all_teamstats)
        ensure_csv(TEAM_CSV, list(team_df.columns))
        upsert_csv(TEAM_CSV, team_df, dedupe_keys=["gameId", "teamId"])
    else:
        print("[warn] no team rows collected")

    print("[done] export complete")

if __name__ == "__main__":
    main()
