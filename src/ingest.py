#!/usr/bin/env python3
"""
NBA ingest script with legacy fallback.
- Recent seasons: https://cdn.nba.com/static/json/liveData/...
- Older seasons:  https://data.nba.net/10s/prod/v1|v2/<YYYYMMDD>/scoreboard.json
                  https://data.nba.net/10s/prod/v1|v2/<YYYYMMDD>/<GAMEID>_boxscore.json

Outputs (CSV in export/):
  games.csv        (one row per game)
  team_stats.csv   (one row per team per game)
  player_stats.csv (one row per player per game)

Date control via env:
  START_DATE / END_DATE (YYYY-MM-DD, inclusive)  OR  DAYS_BACK=N
Default window: yesterday + today (ET).
"""

from __future__ import annotations
import csv, os, sys, time
from typing import Dict, Any, List
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
import requests
import pandas as pd

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome Safari",
    "Accept": "application/json,text/plain,*/*",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com",
}

ET = ZoneInfo("America/New_York")
EXPORT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "export"))
os.makedirs(EXPORT_DIR, exist_ok=True)

GAMES_CSV   = os.path.join(EXPORT_DIR, "games.csv")
TEAM_CSV    = os.path.join(EXPORT_DIR, "team_stats.csv")
PLAYERS_CSV = os.path.join(EXPORT_DIR, "player_stats.csv")

USE_PARQUET = os.environ.get("USE_PARQUET", "0") == "1"

# ------------------------ helpers ------------------------

def et_now_date() -> date:
    return datetime.now(tz=ET).date()

def date_yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"

def daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d = d + timedelta(days=1)

def http_get_json(url: str) -> Dict[str, Any] | None:
    try:
        r = requests.get(url, headers=HEADERS, timeout=30)
        if r.status_code == 200:
            return r.json()
        return None
    except Exception as e:
        print(f"[warn] GET failed {url}: {e}", file=sys.stderr)
        return None

# ------------------- fetch: scoreboard -------------------

def fetch_scoreboard(d: date) -> Dict[str, Any] | None:
    """Try liveData (recent) then legacy (older)."""
    ymd = date_yyyymmdd(d)
    # liveData first
    live_candidates = [
        f"https://cdn.nba.com/static/json/liveData/scoreboard/scoreboard_{ymd}.json",
    ]
    if d == et_now_date():
        live_candidates.append("https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_00.json")
    for u in live_candidates:
        js = http_get_json(u)
        if js:
            js["_source"], js["_ymd"] = "live", ymd
            return js

    # legacy (correct host: data.nba.net)
    legacy_candidates = [
        f"https://data.nba.net/10s/prod/v1/{ymd}/scoreboard.json",
        f"https://data.nba.net/10s/prod/v2/{ymd}/scoreboard.json",
    ]
    for u in legacy_candidates:
        js = http_get_json(u)
        if js:
            js["_source"], js["_ymd"] = "legacy", ymd
            return js
    return None

# ------------------- parse: scoreboard -------------------

def parse_scoreboard(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Unified per-game dicts; include gameDateYmd for legacy boxscore path."""
    if not js: return []
    src, ymd = js.get("_source"), js.get("_ymd")
    games: List[Dict[str, Any]] = []

    if src == "live":
        sb = js.get("scoreboard", {}) or {}
        for g in sb.get("games", []) or []:
            h, a = (g.get("homeTeam") or {}), (g.get("awayTeam") or {})
            games.append({
                "gameId": g.get("gameId"),
                "gameCode": g.get("gameCode"),
                "gameDateEt": g.get("gameEt"),
                "gameStatusText": g.get("gameStatusText"),
                "period": g.get("period"), "gameClock": g.get("gameClock"),
                "arenaName": g.get("arenaName") or "",
                "homeTeamId": h.get("teamId"), "homeTeamTricode": h.get("teamTricode"), "homeScore": h.get("score"),
                "awayTeamId": a.get("teamId"), "awayTeamTricode": a.get("teamTricode"), "awayScore": a.get("score"),
                "gameDateYmd": ymd, "_source": "live",
            })
    else:
        # legacy: root "games": [ {"gameId","hTeam":{...},"vTeam":{...}, ...} ]
        for g in js.get("games") or []:
            h, v = (g.get("hTeam") or {}), (g.get("vTeam") or {})
            tc = lambda t: t.get("triCode") or t.get("tricode")
            games.append({
                "gameId": g.get("gameId"),
                "gameCode": g.get("gameUrlCode") or g.get("gameCode"),
                "gameDateEt": g.get("startTimeEastern") or g.get("startTimeUTC"),
                "gameStatusText": g.get("statusText") or g.get("gameStatusText"),
                "period": (g.get("period") or {}).get("current"),
                "gameClock": g.get("clock"),
                "arenaName": g.get("arenaName") or "",
                "homeTeamId": h.get("teamId"), "homeTeamTricode": tc(h), "homeScore": h.get("score"),
                "awayTeamId": v.get("teamId"), "awayTeamTricode": tc(v), "awayScore": v.get("score"),
                "gameDateYmd": ymd, "_source": "legacy",
            })
    return [g for g in games if g.get("gameId")]

# -------------------- fetch: boxscore --------------------

def fetch_boxscore(game_id: str, gameDateYmd: str | None) -> Dict[str, Any] | None:
    live = http_get_json(f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json")
    if live:
        live["_source"] = "live"
        return live
    if gameDateYmd:
        for base in ["https://data.nba.net/10s/prod/v1", "https://data.nba.net/10s/prod/v2"]:
            u = f"{base}/{gameDateYmd}/{game_id}_boxscore.json"
            leg = http_get_json(u)
            if leg:
                leg["_source"] = "legacy"
                return leg
    return None

# -------------------- parse: boxscore --------------------

def parse_players_from_boxscore(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not js: return []
    src = js.get("_source")
    rows: List[Dict[str, Any]] = []

    if src == "live":
        game = js.get("game") or {}
        box  = game.get("boxScore") or {}
        players = box.get("players") or []
        game_id = game.get("gameId")
        teams = {t.get("teamTricode"): t for t in (box.get("teams") or [])}
        for p in players:
            tri = p.get("teamTricode")
            teamId = teams.get(tri, {}).get("teamId") if tri in teams else None
            st = p.get("statistics") or {}
            row = {
                "gameId": game_id, "playerId": p.get("personId"),
                "teamId": teamId, "teamTricode": tri,
                "firstName": p.get("firstName"), "familyName": p.get("familyName"),
                "jerseyNum": p.get("jerseyNum"), "position": p.get("position"),
                "minutes": st.get("minutes"), "points": st.get("points"),
                "reboundsTotal": st.get("reboundsTotal"), "assists": st.get("assists"),
                "steals": st.get("steals"), "blocks": st.get("blocks"),
                "turnovers": st.get("turnovers"),
                "fieldGoalsMade": st.get("fieldGoalsMade"), "fieldGoalsAttempted": st.get("fieldGoalsAttempted"),
                "threePointersMade": st.get("threePointersMade"), "threePointersAttempted": st.get("threePointersAttempted"),
                "freeThrowsMade": st.get("freeThrowsMade"), "freeThrowsAttempted": st.get("freeThrowsAttempted"),
                "plusMinus": st.get("plusMinusPoints"),
                "didNotPlay": p.get("didNotPlay"), "notPlayingReason": p.get("notPlayingReason"),
            }
            if row["gameId"] and row["playerId"]:
                rows.append(row)
        return rows

    # legacy
    game = js.get("basicGameData") or {}
    stats = js.get("stats") or {}
    game_id = game.get("gameId")
    for p in (stats.get("activePlayers") or []):
        row = {
            "gameId": game_id, "playerId": p.get("personId"),
            "teamId": p.get("teamId"), "teamTricode": None,
            "firstName": None, "familyName": None,
            "jerseyNum": None, "position": None,
            "minutes": p.get("min"), "points": p.get("points"),
            "reboundsTotal": p.get("totReb"), "assists": p.get("assists"),
            "steals": p.get("steals"), "blocks": p.get("blocks"),
            "turnovers": p.get("turnovers"),
            "fieldGoalsMade": p.get("fgm"), "fieldGoalsAttempted": p.get("fga"),
            "threePointersMade": p.get("tpm") or p.get("fg3m"),
            "threePointersAttempted": p.get("tpa") or p.get("fg3a"),
            "freeThrowsMade": p.get("ftm"), "freeThrowsAttempted": p.get("fta"),
            "plusMinus": p.get("plusMinus"),
            "didNotPlay": None, "notPlayingReason": None,
        }
        if row["gameId"] and row["playerId"]:
            rows.append(row)
    return rows

def parse_teams_from_boxscore(js: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not js: return []
    src = js.get("_source")
    rows: List[Dict[str, Any]] = []

    if src == "live":
        game = js.get("game") or {}
        box  = game.get("boxScore") or {}
        teams = box.get("teams") or []
        home, away = (game.get("homeTeam") or {}), (game.get("awayTeam") or {})
        home_id, away_id = home.get("teamId"), away.get("teamId")
        home_score, away_score = (home.get("score") or 0), (away.get("score") or 0)
        id_to_score = {home_id: home_score, away_id: away_score}
        id_to_opp   = {home_id: away_id, away_id: home_id}
        game_id = game.get("gameId")

        for t in teams:
            st = t.get("statistics") or {}
            team_id = t.get("teamId")
            is_home = 1 if team_id == home_id else 0 if team_id == away_id else None
            opp_id  = id_to_opp.get(team_id)
            win = None
            try:
                if team_id in id_to_score and opp_id in id_to_score:
                    win = 1 if int(id_to_score[team_id]) > int(id_to_score[opp_id]) else 0
            except Exception:
                pass
            rows.append({
                "gameId": game_id, "teamId": team_id,
                "teamTricode": t.get("teamTricode"), "teamCity": t.get("teamCity"), "teamName": t.get("teamName"),
                "opponentTeamId": opp_id, "home": is_home, "win": win,
                "points": t.get("score") if t.get("score") is not None else st.get("points"),
                "reboundsTotal": st.get("reboundsTotal"), "assists": st.get("assists"),
                "steals": st.get("steals"), "blocks": st.get("blocks"), "turnovers": st.get("turnovers"),
                "fieldGoalsMade": st.get("fieldGoalsMade"), "fieldGoalsAttempted": st.get("fieldGoalsAttempted"),
                "threePointersMade": st.get("threePointersMade"), "threePointersAttempted": st.get("threePointersAttempted"),
                "freeThrowsMade": st.get("freeThrowsMade"), "freeThrowsAttempted": st.get("freeThrowsAttempted"),
            })
        return rows

    # legacy
    game = js.get("basicGameData") or {}
    stats = js.get("stats") or {}
    game_id = game.get("gameId")

    def team_totals(key):
        t = stats.get(key) or {}
        totals = t.get("totals") or t
        return t, totals

    ht_meta, ht = team_totals("hTeam")
    vt_meta, vt = team_totals("vTeam")

    home_id = (game.get("hTeam") or {}).get("teamId") or ht_meta.get("teamId")
    away_id = (game.get("vTeam") or {}).get("teamId") or vt_meta.get("teamId")

    def score_of(meta, totals):
        return meta.get("score") or totals.get("points") or totals.get("pts")

    home_score, away_score = score_of(ht_meta, ht), score_of(vt_meta, vt)

    def mk(team_key, meta, totals, is_home_flag):
        base = (game.get(team_key) or {})
        team_id = base.get("teamId") or meta.get("teamId")
        opp_id  = away_id if is_home_flag == 1 else home_id if is_home_flag == 0 else None
        win = None
        try:
            if home_score is not None and away_score is not None:
                if is_home_flag == 1:
                    win = 1 if int(home_score) > int(away_score) else 0
                else:
                    win = 1 if int(away_score) > int(home_score) else 0
        except Exception:
            pass
        return {
            "gameId": game_id, "teamId": team_id,
            "teamTricode": base.get("triCode") or base.get("tricode"),
            "teamCity": base.get("teamCity"), "teamName": base.get("teamName"),
            "opponentTeamId": opp_id, "home": is_home_flag, "win": win,
            "points": score_of(meta, totals),
            "reboundsTotal": totals.get("totReb") or totals.get("reboundsTotal"),
            "assists": totals.get("assists"), "steals": totals.get("steals"),
            "blocks": totals.get("blocks"), "turnovers": totals.get("turnovers"),
            "fieldGoalsMade": totals.get("fgm") or totals.get("fieldGoalsMade"),
            "fieldGoalsAttempted": totals.get("fga") or totals.get("fieldGoalsAttempted"),
            "threePointersMade": totals.get("tpm") or totals.get("fg3m") or totals.get("threePointersMade"),
            "threePointersAttempted": totals.get("tpa") or totals.get("fg3a") or totals.get("threePointersAttempted"),
            "freeThrowsMade": totals.get("ftm") or totals.get("freeThrowsMade"),
            "freeThrowsAttempted": totals.get("fta") or totals.get("freeThrowsAttempted"),
        }

    rows = []
    if home_id: rows.append(mk("hTeam", ht_meta, ht, 1))
    if away_id: rows.append(mk("vTeam", vt_meta, vt, 0))
    return rows

# -------------------- IO helpers --------------------

def ensure_csv(path: str, columns: List[str]):
    if not os.path.exists(path):
        with open(path, "w", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(columns)

def upsert_csv(path: str, df_new: pd.DataFrame, dedupe_keys: List[str]):
    if os.path.exists(path):
        df_old = pd.read_csv(path)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()
    df_all.drop_duplicates(subset=dedupe_keys, keep="last", inplace=True)
    df_all.to_csv(path, index=False)
    if USE_PARQUET:
        try:
            df_all.to_parquet(os.path.splitext(path)[0] + ".parquet", index=False)
        except Exception as e:
            print(f"[warn] parquet write skipped: {e}", file=sys.stderr)

# ------------------------ main ------------------------

def main():
    # build date targets
    start_env, end_env, days_back = os.environ.get("START_DATE"), os.environ.get("END_DATE"), os.environ.get("DAYS_BACK")
    parse = lambda s: datetime.strptime(s, "%Y-%m-%d").date()
    today = et_now_date()

    if start_env and end_env:
        a, b = parse(start_env), parse(end_env)
        if b < a: a, b = b, a
        targets = list(daterange(a, b))
        print(f"[info] backfill range {a}..{b} ({len(targets)} days)")
    elif days_back:
        n = max(1, int(days_back))
        targets = [today - timedelta(days=i) for i in range(n, -1, -1)]
        print(f"[info] last {n} days through {today}")
    else:
        targets = [today - timedelta(days=1), today]
        print(f"[info] window: {targets[0]} & {targets[1]} (ET)")

    # collect games
    all_games: List[Dict[str, Any]] = []
    for d in targets:
        sb = fetch_scoreboard(d)
        games = parse_scoreboard(sb) if sb else []
        for gg in games:
            print(f"[game] {gg.get('gameId')} {gg.get('awayTeamTricode')}@{gg.get('homeTeamTricode')} {gg.get('gameStatusText')} src={gg.get('_source')}")
        all_games.extend(games)
    print(f"[info] total games discovered: {len(all_games)}")

    if all_games:
        games_df = pd.DataFrame([{k: v for k, v in g.items() if not k.startswith('_')} for g in all_games])
        ensure_csv(GAMES_CSV, list(games_df.columns))
        upsert_csv(GAMES_CSV, games_df, dedupe_keys=["gameId"])
    else:
        print("[warn] no games found for target dates")

    # per-game boxscore
    all_players: List[Dict[str, Any]] = []
    all_teams: List[Dict[str, Any]] = []
    for g in all_games:
        gid, ymd = g.get("gameId"), g.get("gameDateYmd")
        if not gid: continue
        bs = fetch_boxscore(gid, ymd)
        if not bs:
            print(f"[warn] no boxscore for {gid}")
            continue
        prows = parse_players_from_boxscore(bs)
        trows = parse_teams_from_boxscore(bs)
        print(f"[info] game {gid}: players={len(prows)} teams={len(trows)} src={bs.get('_source')}")
        all_players.extend(prows); all_teams.extend(trows)
        time.sleep(0.3)

    if all_players:
        pdf = pd.DataFrame(all_players)
        ensure_csv(PLAYERS_CSV, list(pdf.columns))
        upsert_csv(PLAYERS_CSV, pdf, dedupe_keys=["gameId", "playerId"])
    else:
        print("[warn] no player rows collected")

    if all_teams:
        tdf = pd.DataFrame(all_teams)
        ensure_csv(TEAM_CSV, list(tdf.columns))
        upsert_csv(TEAM_CSV, tdf, dedupe_keys=["gameId", "teamId"])
    else:
        print("[warn] no team rows collected")

    print("[done] export complete")

if __name__ == "__main__":
    main()
