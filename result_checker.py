"""
경기 결과 확인 및 픽 적중/실패 판단

flow:
  1. picks_db.json 로드
  2. pending 픽 중 경기 종료 예상 시각(game_time + 4h) 이 지난 것 추출
  3. MLB Stats API 로 결과 조회
  4. pick_side 기준 적중/실패/환불 판단
  5. 텔레그램 알림
  6. picks_db 업데이트

픽 방향 정의:
  away / home   → 해당 팀 모노라인 승리
  over / under  → 합산 득점 vs 라인
  fav           → 정배 팀 -1.5 커버 (2점차 이상 승리)
  dog           → 역배 팀 +1.5 커버 (1점차 이내 패배 또는 승리)
"""

import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

import httpx

import config

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))
PICKS_DB_PATH   = os.path.join(config.DATA_DIR, "picks_db.json")
MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"

LEAGUE_EMOJI = {"MLB": "⚾", "KBO": "🇰🇷", "NPB": "🇯🇵"}


# ─── DB I/O ──────────────────────────────────────────────────────────────────

def load_picks_db() -> dict:
    try:
        with open(PICKS_DB_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {"picks": {}}


def save_picks_db(db: dict) -> None:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    with open(PICKS_DB_PATH, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)


def save_pick(key: str, pick_data: dict) -> None:
    """신호 발송 시 픽 저장"""
    db = load_picks_db()
    if key not in db["picks"]:          # 중복 방지: 이미 있으면 덮어쓰지 않음
        db["picks"][key] = pick_data
        save_picks_db(db)


# ─── 팀명 매칭 ───────────────────────────────────────────────────────────────

def _team_match(stored: str, api_name: str) -> bool:
    """
    BetConstruct 팀명 vs MLB Stats API 팀명 부분 매칭.
    ex) "Los Angeles Dodgers" ↔ "Los Angeles Dodgers"
    """
    s = stored.lower().strip()
    a = api_name.lower().strip()
    if not s or not a:
        return False
    # 완전 일치
    if s == a:
        return True
    # 마지막 단어(닉네임) 일치 (Dodgers == Dodgers)
    if s.split()[-1] == a.split()[-1]:
        return True
    # 부분 포함
    if s in a or a in s:
        return True
    return False


# ─── MLB Stats API ───────────────────────────────────────────────────────────

def _mlb_result(away_team: str, home_team: str, date_str: str) -> Optional[dict]:
    """
    MLB Stats API 에서 완료된 경기 스코어 조회.
    date_str: "YYYY-MM-DD"
    반환: {"away_score": int, "home_score": int} 또는 None
    """
    try:
        r = httpx.get(
            MLB_SCHEDULE_URL,
            params={"sportId": 1, "date": date_str},
            timeout=15,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        for date_obj in data.get("dates", []):
            for game in date_obj.get("games", []):
                status = game.get("status", {}).get("abstractGameState", "")
                if status != "Final":
                    continue
                teams = game.get("teams", {})
                a_name = teams.get("away", {}).get("team", {}).get("name", "")
                h_name = teams.get("home", {}).get("team", {}).get("name", "")
                if _team_match(away_team, a_name) and _team_match(home_team, h_name):
                    return {
                        "away_score": int(teams.get("away", {}).get("score", 0) or 0),
                        "home_score": int(teams.get("home", {}).get("score", 0) or 0),
                    }
    except Exception as e:
        logger.error(f"MLB API 조회 오류: {e}")
    return None


def _fetch_result(pick: dict) -> Optional[dict]:
    """리그별 결과 조회 분기"""
    league = pick.get("league", "MLB")
    game_dt_str = pick.get("game_date", "")   # "YYYY-MM-DD"

    if league == "MLB":
        res = _mlb_result(pick["away_team"], pick["home_team"], game_dt_str)
        if res is None and game_dt_str:
            # 날짜 오프셋 ±1 재시도 (UTC 날짜 차이)
            from datetime import date
            try:
                d = datetime.strptime(game_dt_str, "%Y-%m-%d").date()
                for delta in (-1, 1):
                    alt = (d + timedelta(days=delta)).strftime("%Y-%m-%d")
                    res = _mlb_result(pick["away_team"], pick["home_team"], alt)
                    if res:
                        break
            except ValueError:
                pass
        return res
    # KBO / NPB: 추후 구현
    return None


# ─── 적중 판단 ────────────────────────────────────────────────────────────────

def _determine_result(pick: dict, scores: dict) -> Optional[str]:
    """
    픽 + 경기 스코어 → "win" / "loss" / "push"
    """
    pick_side  = pick.get("pick_side", "")
    market     = pick.get("market", "")
    away_score = scores.get("away_score", 0)
    home_score = scores.get("home_score", 0)
    total      = away_score + home_score

    # ── 승패 ──────────────────────────────────────────
    if "승패" in market:
        if pick_side == "away":
            if away_score > home_score:  return "win"
            if away_score < home_score:  return "loss"
            return "push"
        if pick_side == "home":
            if home_score > away_score:  return "win"
            if home_score < away_score:  return "loss"
            return "push"

    # ── 언오버 ────────────────────────────────────────
    elif pick_side in ("over", "under"):
        line = pick.get("ou_line")
        if line is None:
            # market 문자열에서 추출: "언오버 오버(8.5)"
            m = re.search(r"\((\d+\.?\d*)\)", market)
            line = float(m.group(1)) if m else None
        if line is None:
            return None
        line = float(line)
        if pick_side == "over":
            if total > line:   return "win"
            if total < line:   return "loss"
            return "push"
        else:  # under
            if total < line:   return "win"
            if total > line:   return "loss"
            return "push"

    # ── 핸디캡 (-1.5 / +1.5) ─────────────────────────
    elif pick_side in ("fav", "dog"):
        # fav_team_side: "home" or "away" (시장 레이블에서 팀명으로 추정)
        # 단순화: fav = 이기는 팀 -1.5, dog = 지는 팀 +1.5
        fav_side = pick.get("fav_team_side", "")   # "home" or "away"
        if not fav_side:
            return None
        if fav_side == "home":
            fav_margin = home_score - away_score
        else:
            fav_margin = away_score - home_score

        if pick_side == "fav":
            if fav_margin >= 2:   return "win"
            if fav_margin <= 1:   return "loss"   # 0 or negative = loss (push at 0 실제로 없음)
            return "push"
        else:  # dog (+1.5)
            if fav_margin <= 1:   return "win"
            if fav_margin >= 2:   return "loss"
            return "push"

    return None


# ─── 픽 레이블 ───────────────────────────────────────────────────────────────

def pick_label(pick: dict) -> str:
    side = pick.get("pick_side", "")
    if side == "away":
        return f"🏃 어웨이  {pick.get('away_team', '')}"
    if side == "home":
        return f"🏠 홈  {pick.get('home_team', '')}"
    if side == "over":
        line = pick.get("ou_line", "")
        return f"📈 오버 {line}"
    if side == "under":
        line = pick.get("ou_line", "")
        return f"📉 언더 {line}"
    if side == "fav":
        return "📌 정배 (-1.5)"
    if side == "dog":
        return "🐶 역배 (+1.5)"
    return side


# ─── 메인 체커 ────────────────────────────────────────────────────────────────

async def check_results(send_fn) -> None:
    """
    pending 픽 결과 확인 → 텔레그램 알림.
    send_fn: async (text: str) → bool
    """
    db   = load_picks_db()
    picks = db.get("picks", {})
    now  = datetime.now(KST)
    updated = False

    for key, pick in picks.items():
        if pick.get("result") is not None:
            continue   # 이미 처리됨

        # 경기 종료 예상 시각 계산
        end_dt_str = pick.get("check_after", "")
        try:
            end_dt = datetime.strptime(end_dt_str, "%Y-%m-%d %H:%M KST").replace(tzinfo=KST)
        except (ValueError, TypeError):
            continue

        if now < end_dt:
            continue   # 아직 경기 중 또는 대기

        # 결과 조회
        scores = _fetch_result(pick)
        if scores is None:
            # 최대 24시간 이후에도 없으면 포기
            try:
                sent_dt = datetime.strptime(pick.get("sent_at", ""), "%Y-%m-%d %H:%M KST").replace(tzinfo=KST)
                if (now - sent_dt).total_seconds() > 86400:
                    pick["result"] = "unknown"
                    updated = True
            except (ValueError, TypeError):
                pass
            continue

        outcome = _determine_result(pick, scores)
        if outcome is None:
            continue

        pick["result"]       = outcome
        pick["result_score"] = f"{scores['away_score']}-{scores['home_score']}"
        updated = True

        # 텔레그램 알림
        league = pick.get("league", "MLB")
        le     = LEAGUE_EMOJI.get(league, "⚾")
        emoji  = "✅" if outcome == "win" else ("↩️" if outcome == "push" else "❌")
        label  = pick_label(pick)

        msg = (
            f"{emoji} *[결과 확인]*\n"
            f"\n"
            f"{le} {league}: *{pick.get('away_team','')} vs {pick.get('home_team','')}*\n"
            f"⏰ 경기: {pick.get('game_time','')}\n"
            f"\n"
            f"📊 {pick.get('market','')}\n"
            f"🎯 픽: {label}\n"
            f"📡 신호 종류: {pick.get('signal_type','')}\n"
            f"\n"
            f"최종 스코어: {scores['away_score']} - {scores['home_score']}\n"
            f"\n"
            f"{'✅ 적중!' if outcome == 'win' else ('↩️ 환불 (push)' if outcome == 'push' else '❌ 실패')}"
        )
        try:
            await send_fn(msg)
        except Exception as e:
            logger.error(f"결과 알림 전송 오류: {e}")

        logger.info(
            f"결과: [{outcome.upper()}] {pick.get('away_team')} vs {pick.get('home_team')} "
            f"| {pick.get('market')} | 픽={pick.get('pick_side')} "
            f"| 스코어={scores['away_score']}-{scores['home_score']}"
        )

    if updated:
        save_picks_db(db)
