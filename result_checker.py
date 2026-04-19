"""
경기 결과 확인 및 픽 적중/실패 판단

BetConstruct WebSocket 에서 직접 최종 스코어 조회
→ 팀명 100% 일치, MLB/KBO/NPB 모든 리그 지원
  (bwzkix1.com/경기결과 와 동일한 백엔드)

flow:
  1. picks_db.json 로드
  2. pending 픽 중 check_after 시각 지난 것 추출
  3. BetConstruct WebSocket 으로 해당 match_id 스코어 조회
  4. pick_side 기준 적중/실패/환불 판단
  5. 텔레그램 알림
  6. picks_db 업데이트

pick_side 정의:
  away / home   → 해당 팀 모노라인 승리
  over / under  → 합산 득점 vs 라인
  fav           → 정배 팀 -1.5 커버 (2점차 이상 승리)
  dog           → 역배 팀 +1.5 커버 (1점차 이내 패배 or 승리)
"""

import asyncio
import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta
from typing import Optional

import websockets

import config

logger = logging.getLogger(__name__)

KST           = timezone(timedelta(hours=9))
PICKS_DB_PATH = os.path.join(config.DATA_DIR, "picks_db.json")
LEAGUE_EMOJI  = {
    "MLB": "⚾", "KBO": "🇰🇷", "NPB": "🇯🇵",
    "EPL": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "Bundesliga": "🇩🇪", "SerieA": "🇮🇹", "Ligue1": "🇫🇷",
    "LaLiga": "🇪🇸", "UCL": "🏆",
}

WS_URL  = "wss://eu-swarm-springre.betconstruct.com/"
SITE_ID = "18747716"
AFEC    = "dcnwYYDt8VI9EMDflnoY8j-qw919zRj47uOK"


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
    """신호 발송 시 픽 저장 (중복 방지)"""
    db = load_picks_db()
    if key not in db["picks"]:
        db["picks"][key] = pick_data
        save_picks_db(db)


# ─── BetConstruct WebSocket 스코어 조회 ──────────────────────────────────────

async def _ws_get_score(match_id: str) -> Optional[dict]:
    """
    BetConstruct WebSocket 에서 특정 match_id 의 최종 스코어 조회.
    반환: {"team1_score": int, "team2_score": int, "is_finished": bool} 또는 None
    team1 = 홈, team2 = 어웨이  (BetConstruct 규칙)
    """
    rid_session = "rc_session"
    rid_get     = "rc_get"

    try:
        async with websockets.connect(WS_URL, open_timeout=10, ping_interval=None) as ws:
            # 1) 세션 초기화
            await ws.send(json.dumps({
                "command": "request_session",
                "params":  {"site_id": SITE_ID, "afec": AFEC, "language": "en"},
                "rid":     rid_session,
            }))
            # 세션 응답 수신
            for _ in range(5):
                raw = await asyncio.wait_for(ws.recv(), timeout=8)
                msg = json.loads(raw)
                if msg.get("rid") == rid_session:
                    break

            # 2) 이벤트 결과 조회
            await ws.send(json.dumps({
                "command": "get",
                "params": {
                    "source": "betting",
                    "what": {
                        "event": [
                            "id", "team1_name", "team2_name",
                            "score1", "score2",          # 최종 스코어
                            "add_info",                  # 진행 상태 등
                            "type",                      # 1=예정 2=라이브 3=종료
                            "info",
                        ]
                    },
                    "where": {
                        "event": {"id": int(match_id)}
                    }
                },
                "rid": rid_get,
            }))

            # 응답 수신 (최대 5번 시도)
            for _ in range(8):
                raw = await asyncio.wait_for(ws.recv(), timeout=8)
                msg = json.loads(raw)
                if msg.get("rid") != rid_get:
                    continue
                data = msg.get("data", {}).get("data", {})
                events = data.get("event", {})
                if not events:
                    return None
                ev = next(iter(events.values())) if isinstance(events, dict) else events[0]
                score1 = ev.get("score1")   # 홈
                score2 = ev.get("score2")   # 어웨이
                ev_type = ev.get("type", 0)  # 3 = 종료

                # 스코어가 없거나 경기 미종료면 None
                if score1 is None or score2 is None:
                    return None

                try:
                    home_score = int(str(score1).split(":")[0])
                    away_score = int(str(score2).split(":")[0])
                except (ValueError, IndexError):
                    # score1/score2 가 "3:2" 형태로 올 때 파싱
                    try:
                        parts = str(score1).split(":")
                        home_score = int(parts[0])
                        away_score = int(parts[1]) if len(parts) > 1 else int(str(score2))
                    except Exception:
                        return None

                return {
                    "home_score":  home_score,
                    "away_score":  away_score,
                    "is_finished": ev_type == 3,
                }

    except Exception as e:
        logger.error(f"BetConstruct 결과 조회 오류 (match_id={match_id}): {e}")
    return None


# ─── 적중 판단 ────────────────────────────────────────────────────────────────

def _determine_result(pick: dict, scores: dict) -> Optional[str]:
    """
    pick_side + 스코어 → "win" / "loss" / "push"
    """
    pick_side  = pick.get("pick_side", "")
    market     = pick.get("market", "")
    away_score = scores.get("away_score", 0)
    home_score = scores.get("home_score", 0)
    total      = away_score + home_score

    # ── 승패 ──────────────────────────────────────────────────────
    if "승패" in market:
        if pick_side == "away":
            if away_score > home_score: return "win"
            if away_score < home_score: return "loss"
            return "push"
        if pick_side == "home":
            if home_score > away_score: return "win"
            if home_score < away_score: return "loss"
            return "push"

    # ── 언오버 ────────────────────────────────────────────────────
    elif pick_side in ("over", "under"):
        line = pick.get("ou_line")
        if line is None:
            m = re.search(r"\((\d+\.?\d*)\)", market)
            line = float(m.group(1)) if m else None
        if line is None:
            return None
        line = float(line)
        if pick_side == "over":
            if total > line: return "win"
            if total < line: return "loss"
            return "push"
        else:
            if total < line: return "win"
            if total > line: return "loss"
            return "push"

    # ── 핸디캡 -1.5 / +1.5 ────────────────────────────────────────
    elif pick_side in ("fav", "dog"):
        fav_side = pick.get("fav_team_side", "")   # "home" or "away"
        if not fav_side:
            return None
        fav_margin = (home_score - away_score) if fav_side == "home" else (away_score - home_score)
        if pick_side == "fav":
            if fav_margin >= 2: return "win"
            return "loss"
        else:  # dog +1.5
            if fav_margin <= 1: return "win"
            return "loss"

    return None


# ─── 픽 레이블 ───────────────────────────────────────────────────────────────

def pick_label(pick: dict) -> str:
    side = pick.get("pick_side", "")
    if side == "away":  return f"🏃 어웨이  {pick.get('away_team', '')}"
    if side == "home":  return f"🏠 홈  {pick.get('home_team', '')}"
    if side == "over":  return f"📈 오버 {pick.get('ou_line', '')}"
    if side == "under": return f"📉 언더 {pick.get('ou_line', '')}"
    if side == "fav":   return "📌 정배 (-1.5)"
    if side == "dog":   return "🐶 역배 (+1.5)"
    return side


# ─── 메인 체커 ────────────────────────────────────────────────────────────────

async def check_results(send_fn) -> None:
    """
    pending 픽 결과 확인 → 텔레그램 알림.
    send_fn: async (text: str) → bool
    """
    db    = load_picks_db()
    picks = db.get("picks", {})
    now   = datetime.now(KST)
    updated = False

    pending = [
        (key, pick) for key, pick in picks.items()
        if pick.get("result") is None
    ]
    if not pending:
        return

    # pending 픽 중 check_after 지난 것만 처리
    to_check = []
    for key, pick in pending:
        try:
            end_dt = datetime.strptime(
                pick.get("check_after", ""), "%Y-%m-%d %H:%M KST"
            ).replace(tzinfo=KST)
            if now >= end_dt:
                to_check.append((key, pick))
        except (ValueError, TypeError):
            continue

    if not to_check:
        return

    logger.info(f"결과 확인 대상: {len(to_check)}건")

    for key, pick in to_check:
        match_id = pick.get("match_id", "")
        if not match_id:
            continue

        scores = await _ws_get_score(str(match_id))

        # 스코어 없거나 아직 미종료
        if scores is None or not scores.get("is_finished", False):
            # 발송 후 24시간 초과 → 포기
            try:
                sent_dt = datetime.strptime(
                    pick.get("sent_at", ""), "%Y-%m-%d %H:%M KST"
                ).replace(tzinfo=KST)
                if (now - sent_dt).total_seconds() > 86400:
                    pick["result"] = "unknown"
                    updated = True
                    logger.warning(f"결과 포기 (24h 초과): {pick.get('away_team')} vs {pick.get('home_team')}")
            except (ValueError, TypeError):
                pass
            continue

        outcome = _determine_result(pick, scores)
        if outcome is None:
            logger.warning(f"결과 판단 불가: {pick.get('away_team')} vs {pick.get('home_team')} / {pick.get('pick_side')}")
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
            f"스코어: {pick.get('away_team','')} *{scores['away_score']}* - *{scores['home_score']}* {pick.get('home_team','')}\n"
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
