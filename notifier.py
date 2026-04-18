"""
텔레그램 알림 발송

중복 방지: (match_id + market + signal_type + opening_val) 해시
"""

import hashlib
import json
import logging
import os

import httpx

import config
from analyzer import Signal

logger = logging.getLogger(__name__)

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
    "MLB": "⚾",
    "KBO": "🇰🇷",
    "NPB": "🇯🇵",
}


def _build_message(sig: Signal) -> str:
    emoji  = SIGNAL_EMOJI.get(sig.signal_type, "📊")
    hours  = f"{sig.hours_left:.1f}h" if sig.hours_left is not None else "?"
    league = getattr(sig, "league", "MLB")
    le     = LEAGUE_EMOJI.get(league, "⚾")
    tag    = f"{le} {league}"

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
    _save_sent(sent)
    return count
