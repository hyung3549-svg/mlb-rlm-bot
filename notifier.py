"""
텔레그램 알림 발송

중복 방지: (match_id + market + signal_type + opening_val) 해시
"""

import hashlib
import json
import logging
import os
import re
from datetime import datetime, timezone, timedelta

import httpx

import config
from analyzer import Signal
import result_checker

logger = logging.getLogger(__name__)

KST        = timezone(timedelta(hours=9))
SENT_CACHE = os.path.join(config.DATA_DIR, "sent_signals.json")

SIGNAL_EMOJI = {
    "NEW_GAME":  "🆕",
    "RLM":       "🚨",
    "LINE_RLM":  "🚨📐",
    "STEAM":     "⚡",
    "FINAL":     "🏁",
}


def _load_sent() -> set:
    try:
        with open(SENT_CACHE, encoding="utf-8") as f:
            return set(json.load(f))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


def _save_sent(sent: set) -> None:
    os.makedirs(config.DATA_DIR, exist_ok=True)
    with open(SENT_CACHE, "w", encoding="utf-8") as f:
        json.dump(list(sent), f)


def _key(sig: Signal) -> str:
    raw = f"{sig.match_id}:{sig.signal_type}:{sig.market}:{sig.opening_val}"
    return hashlib.md5(raw.encode()).hexdigest()


LEAGUE_EMOJI = {
    "MLB":        "⚾",
    "KBO":        "🇰🇷",
    "NPB":        "🇯🇵",
    "EPL":        "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
    "Bundesliga": "🇩🇪",
    "SerieA":     "🇮🇹",
    "Ligue1":     "🇫🇷",
    "LaLiga":       "🇪🇸",
    "UCL":          "🏆",
    "Championship": "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
}


def _pick_line(sig: Signal) -> str:
    """픽 방향 한 줄 문자열. pick_side 없으면 빈 문자열."""
    side = getattr(sig, "pick_side", "")
    if not side:
        return ""
    if side == "away":
        return f"\n🎯 픽: 어웨이  *{sig.away_team}*"
    if side == "home":
        return f"\n🎯 픽: 홈  *{sig.home_team}*"
    if side == "draw":
        return f"\n🎯 픽: 무승부 X"
    if side == "over":
        m = re.search(r"\((\d+\.?\d*)\)", sig.market)
        line = m.group(1) if m else ""
        return f"\n🎯 픽: 오버 {line}"
    if side == "under":
        m = re.search(r"\((\d+\.?\d*)\)", sig.market)
        line = m.group(1) if m else ""
        return f"\n🎯 픽: 언더 {line}"
    if side == "fav":
        return "\n🎯 픽: 정배 (-1.5)"
    if side == "dog":
        return "\n🎯 픽: 역배 (+1.5)"
    return ""


def _build_message(sig: Signal) -> str:
    emoji  = SIGNAL_EMOJI.get(sig.signal_type, "📊")
    hours  = f"{sig.hours_left:.1f}h" if sig.hours_left is not None else "?"
    league = getattr(sig, "league", "MLB")
    le     = LEAGUE_EMOJI.get(league, "⚾")
    tag    = f"{le} {league}"
    pick   = _pick_line(sig)

    if sig.signal_type == "NEW_GAME":
        return (
            f"{emoji} *[신규 경기 등록]*\n"
            f"\n"
            f"{tag}: *{sig.away_team} vs {sig.home_team}*\n"
            f"⏰ 경기: {sig.game_time}  (약 {hours} 후)\n"
            f"\n"
            f"📌 오프닝 라인 고정\n"
            f"{sig.description}"
        )

    if sig.signal_type == "STEAM":
        return (
            f"{emoji} *[스팀무브 감지 — 즉시 알림]*\n"
            f"\n"
            f"{tag}: *{sig.away_team} vs {sig.home_team}*\n"
            f"⏰ 경기: {sig.game_time}  (약 {hours} 후)\n"
            f"\n"
            f"📊 항목: {sig.market}\n"
            f"💥 {sig.description}\n"
            f"  15분 전: {sig.opening_val}  →  현재: {sig.current_val}  ({sig.change_val})"
            f"{pick}"
        )

    if sig.signal_type == "LINE_MOVE":
        money_line = f"\n💵 {sig.money_pct}" if sig.money_pct else ""
        return (
            f"📐 *[언오버 라인 이동]*\n"
            f"\n"
            f"{tag}: *{sig.away_team} vs {sig.home_team}*\n"
            f"⏰ 경기: {sig.game_time}  (약 {hours} 후)\n"
            f"\n"
            f"📐 {sig.description}\n"
            f"  오프닝: {sig.opening_val}  →  현재: {sig.current_val}  ({sig.change_val})"
            f"{money_line}"
        )

    if sig.signal_type == "LINE_RLM":
        money_line = f"\n💵 {sig.money_pct}" if sig.money_pct else ""
        return (
            f"{emoji} *[라인 역행 감지 — $% 반대 이동]*\n"
            f"\n"
            f"{tag}: *{sig.away_team} vs {sig.home_team}*\n"
            f"⏰ 경기: {sig.game_time}  (약 {hours} 후)\n"
            f"\n"
            f"📐 {sig.description}\n"
            f"  오프닝: {sig.opening_val}  →  현재: {sig.current_val}  ({sig.change_val})"
            f"{money_line}"
            f"{pick}"
        )

    if sig.signal_type == "FINAL":
        money_line = f"\n💵 {sig.money_pct}" if sig.money_pct else ""
        return (
            f"{emoji} *[최종 판단 — 경기 {hours} 전]*\n"
            f"\n"
            f"{tag}: *{sig.away_team} vs {sig.home_team}*\n"
            f"⏰ 경기: {sig.game_time}\n"
            f"\n"
            f"📊 항목: {sig.market}\n"
            f"🏁 {sig.description}\n"
            f"  오프닝: {sig.opening_val}  →  현재: {sig.current_val}  ({sig.change_val})"
            f"{money_line}"
            f"{pick}"
        )

    # RLM
    money_line = f"\n💵 {sig.money_pct}" if sig.money_pct else ""
    return (
        f"{emoji} *[배당 역행 확인]*\n"
        f"\n"
        f"{tag}: *{sig.away_team} vs {sig.home_team}*\n"
        f"⏰ 경기: {sig.game_time}  (약 {hours} 후)\n"
        f"\n"
        f"📊 항목: {sig.market}\n"
        f"📉 {sig.description}\n"
        f"  오프닝: {sig.opening_val}  →  현재: {sig.current_val}  ({sig.change_val})"
        f"{money_line}"
        f"{pick}"
    )


async def _send(text: str) -> bool:
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("\n" + "=" * 55)
        print(text)
        print("=" * 55 + "\n")
        return True
    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, json={
                "chat_id":    config.TELEGRAM_CHAT_ID,
                "text":       text,
                "parse_mode": "Markdown",
            })
            return r.status_code == 200
    except Exception as e:
        logger.error(f"텔레그램 전송 오류: {e}")
        return False


def _build_pick_record(sig: Signal, key: str) -> dict:
    """픽 DB 저장 레코드 생성"""
    now_kst = datetime.now(KST)
    now_str = now_kst.strftime("%Y-%m-%d %H:%M KST")

    # 경기 시작 시각 계산 (KST)
    game_dt = None
    try:
        hm = sig.game_time.replace(" KST", "").strip()
        hh, mm = int(hm[:2]), int(hm[3:5])
        cand = now_kst.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if cand <= now_kst:
            cand += __import__("datetime").timedelta(days=1)
        game_dt = cand
    except Exception:
        pass

    game_date_str   = game_dt.strftime("%Y-%m-%d") if game_dt else ""
    # 경기 종료 예상 = 시작 + 4시간
    check_after_str = (game_dt + timedelta(hours=4)).strftime("%Y-%m-%d %H:%M KST") if game_dt else ""

    # OU 라인 추출
    ou_line = None
    if sig.pick_side in ("over", "under"):
        m = re.search(r"\((\d+\.?\d*)\)", sig.market)
        if m:
            ou_line = float(m.group(1))

    return {
        "key":          key,
        "match_id":     sig.match_id,
        "away_team":    sig.away_team,
        "home_team":    sig.home_team,
        "game_time":    sig.game_time,
        "game_date":    game_date_str,
        "signal_type":  sig.signal_type,
        "market":       sig.market,
        "pick_side":    sig.pick_side,
        "ou_line":      ou_line,
        "league":       getattr(sig, "league", "MLB"),
        "sent_at":      now_str,
        "check_after":  check_after_str,
        "result":       None,
    }


async def notify(signals: list[Signal]) -> int:
    sent = _load_sent()
    count = 0
    for sig in signals:
        k = _key(sig)
        if k in sent:
            continue
        msg = _build_message(sig)
        ok  = await _send(msg)
        if ok:
            sent.add(k)
            count += 1
            logger.info(f"알림: [{sig.signal_type}] {sig.away_team} vs {sig.home_team} / {sig.market}")
            # 픽이 있는 신호만 picks_db 에 저장
            if getattr(sig, "pick_side", ""):
                try:
                    record = _build_pick_record(sig, k)
                    result_checker.save_pick(k, record)
                except Exception as e:
                    logger.error(f"픽 저장 오류: {e}")
    _save_sent(sent)
    return count
