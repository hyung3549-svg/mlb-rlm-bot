"""
게임 DB 관리

구조:
  games_db.json
  └── games
      └── {match_id}
          ├── away_team / home_team / game_time_kst / url
          ├── first_seen        ← 최초 수집 시각
          ├── opening           ← 최초 수집 배당 (절대 덮어쓰지 않음)
          │   ├── moneyline
          │   ├── handicap_15
          │   └── main_ou
          ├── current           ← 가장 최근 배당
          └── history[]         ← 최근 스냅샷 (스팀무브 감지용)
"""

import json
import os
from datetime import datetime, timezone, timedelta

import config

KST          = timezone(timedelta(hours=9))
DB_PATH      = os.path.join(config.DATA_DIR, "games_db.json")
HISTORY_MAX  = 12   # 5분 간격 × 12 = 최근 60분치 유지


def load_db() -> dict:
    try:
        with open(DB_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"games": {}}


def save_db(db: dict) -> None:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    with open(DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def _now_kst() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")


def _make_snapshot(game: dict) -> dict:
    return {
        "moneyline":   game.get("moneyline"),
        "handicap_15": game.get("handicap_15"),
        "main_ou":     game.get("main_ou"),
        "fetched_at":  game.get("fetched_at", _now_kst()),
    }


def _find_duplicate(games: dict, game: dict) -> str | None:
    """
    같은 리그 + 같은 팀 조합의 기존 match_id 반환 (중복 방지).
    """
    league    = game.get("league", "MLB")
    away_team = game.get("away_team", "")
    home_team = game.get("home_team", "")
    new_id    = game["match_id"]

    for gid, entry in games.items():
        if gid == new_id:
            continue
        if (entry.get("league", "MLB") == league
                and entry.get("away_team") == away_team
                and entry.get("home_team") == home_team):
            return gid
    return None


def update_game(db: dict, game: dict) -> dict:
    """
    신규 게임: opening = current = 첫 스냅샷  (오프닝 라인 고정)
    기존 게임: opening 절대 불변, current 갱신, history 추가
    반환: 해당 게임 DB 엔트리 (is_new 플래그 포함)
    """
    gid   = game["match_id"]
    games = db.setdefault("games", {})
    snap  = _make_snapshot(game)

    # 같은 팀 중복 엔트리 감지 → 기존 ID로 병합
    dup_id = _find_duplicate(games, game)
    if dup_id and gid not in games:
        games[gid] = games.pop(dup_id)  # 새 ID로 이전

    is_new = gid not in games

    if is_new:
        games[gid] = {
            "away_team":     game.get("away_team", ""),
            "home_team":     game.get("home_team", ""),
            "game_time_kst": game.get("game_time_kst", ""),
            "league":        game.get("league", "MLB"),
            "url":           game.get("url", ""),
            "first_seen":    _now_kst(),
            "opening":       snap,
            "current":       snap,
            "history":       [snap],
            "is_new":        True,
        }
    else:
        entry = games[gid]
        # 팀명·시간·리그 보완
        if game.get("away_team"):
            entry["away_team"] = game["away_team"]
        if game.get("home_team"):
            entry["home_team"] = game["home_team"]
        if game.get("game_time_kst"):
            entry["game_time_kst"] = game["game_time_kst"]
        if game.get("league"):
            entry["league"] = game["league"]

        entry["current"] = snap
        entry.setdefault("history", []).append(snap)
        if len(entry["history"]) > HISTORY_MAX:
            entry["history"] = entry["history"][-HISTORY_MAX:]
        entry["is_new"] = False

    return games[gid]


def hours_until_game(entry: dict) -> float | None:
    """
    KST 기준 경기 시작까지 남은 시간(시간 단위) 계산.
    게임 시간 형식: "02:35 KST"
    날짜는 '지금보다 미래인 가장 가까운 날짜'로 결정.
    """
    raw = entry.get("game_time_kst", "")
    try:
        time_str = raw.replace(" KST", "").strip()   # "02:35"
        hh, mm   = int(time_str[:2]), int(time_str[3:5])
    except (ValueError, IndexError):
        return None

    now = datetime.now(KST)
    candidate = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)   # 오늘 이미 지났으면 내일

    delta_hours = (candidate - now).total_seconds() / 3600
    return round(delta_hours, 2)
