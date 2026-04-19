"""
텔레그램 ↔ Claude AI 채팅 인터페이스

Railway 에서 24/7 실행 — 컴퓨터 꺼져도 대화 가능.
허용된 TELEGRAM_CHAT_ID 에서만 응답 (보안).

사용 예시:
  "오늘 픽 뭐야?"
  "현재 경기 몇개야?"
  "스팀 임계값 0.08로 바꿔줘"
  "배당역행이 뭐야?"
"""

import asyncio
import json
import logging
import os
from collections import defaultdict, deque
from datetime import datetime, timezone, timedelta

import httpx

import config
import db as DB
import result_checker

logger = logging.getLogger(__name__)

KST      = timezone(timedelta(hours=9))
TG_BASE  = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}"
ALLOWED  = {str(config.TELEGRAM_CHAT_ID)}
MAX_HIST = 20   # 최근 N 턴 유지

_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=MAX_HIST * 2))


# ─── 봇 현재 상태 요약 ───────────────────────────────────────────────────────

def _bot_status_summary() -> str:
    """현재 DB 상태 한 줄 요약 — 시스템 프롬프트에 주입"""
    try:
        db    = DB.load_db()
        games = db.get("games", {})
        now   = datetime.now(KST)
        upcoming = sum(
            1 for e in games.values()
            if (h := DB.hours_until_game(e)) is not None and 0 <= h <= 12
        )
        picks_db  = result_checker.load_picks_db()
        picks     = picks_db.get("picks", {})
        pending   = sum(1 for p in picks.values() if p.get("result") is None)
        wins      = sum(1 for p in picks.values() if p.get("result") == "win")
        losses    = sum(1 for p in picks.values() if p.get("result") == "loss")
        total_res = wins + losses
        wr = f"{wins}/{total_res} ({100*wins//total_res}%)" if total_res else "집계 없음"
        return (
            f"[봇 현황 {now.strftime('%m/%d %H:%M')} KST] "
            f"모니터링 경기: {upcoming}개 | "
            f"대기 픽: {pending}건 | "
            f"적중률: {wr}"
        )
    except Exception:
        return "[봇 현황] 데이터 로드 중"


def _build_system_prompt() -> str:
    return f"""당신은 MLB/KBO/NPB 배당역행(RLM) 감지 봇의 AI 어시스턴트입니다.
봇은 24/7 Railway 클라우드에서 실행되며 5분마다 배당을 수집합니다.

{_bot_status_summary()}

핵심 개념:
- 배당역행(RLM): 공개 $% 75%+ 쏠린 팀의 배당이 오프닝 대비 오히려 상승(악화) → 북메이커가 반대 방향으로 라인 이동 = 샤프머니 존재 → 반대 방향 픽
- 스팀무브(STEAM): 15분 내 배당 급변 (샤프머니 즉시 유입) → 배당 급락한 팀 픽
- 리그: MLB(ID=608), KBO(ID=611), NPB(ID=612)
- 결과 추적: 경기 종료 후 BetConstruct WebSocket으로 자동 스코어 조회 → 적중/실패 판단

답변 규칙:
- 한국어로 간결하게 (텔레그램 특성상 짧게)
- 코드 수정이 필요한 요청은 구체적 방법 안내
- 모르는 건 모른다고 솔직하게""".strip()


# ─── Telegram API 헬퍼 ───────────────────────────────────────────────────────

async def _tg_get(method: str, params: dict = {}) -> dict:
    try:
        async with httpx.AsyncClient(timeout=35) as client:
            r = await client.get(f"{TG_BASE}/{method}", params=params)
            return r.json()
    except Exception as e:
        logger.error(f"TG GET {method} 오류: {e}")
        return {}


async def _tg_post(method: str, data: dict) -> dict:
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.post(f"{TG_BASE}/{method}", json=data)
            return r.json()
    except Exception as e:
        logger.error(f"TG POST {method} 오류: {e}")
        return {}


async def send_message(chat_id: str, text: str, reply_to: int = None) -> bool:
    payload = {
        "chat_id":    chat_id,
        "text":       text,
        "parse_mode": "Markdown",
    }
    if reply_to:
        payload["reply_to_message_id"] = reply_to
    resp = await _tg_post("sendMessage", payload)
    return resp.get("ok", False)


# ─── Claude API ──────────────────────────────────────────────────────────────

async def _ask_claude(chat_id: str, user_text: str) -> str:
    if not config.ANTHROPIC_API_KEY:
        return "⚠️ ANTHROPIC\\_API\\_KEY 가 Railway 환경 변수에 설정되지 않았습니다."

    # 히스토리 구성
    hist = list(_history[chat_id])
    hist.append({"role": "user", "content": user_text})

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         config.ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      "claude-3-5-haiku-20241022",
                    "max_tokens": 1024,
                    "system":     _build_system_prompt(),
                    "messages":   hist,
                },
            )
        data  = r.json()
        reply = data["content"][0]["text"]
    except Exception as e:
        logger.error(f"Claude API 오류: {e}")
        return f"❌ Claude 응답 오류: {e}"

    # 히스토리 저장
    _history[chat_id].append({"role": "user",      "content": user_text})
    _history[chat_id].append({"role": "assistant",  "content": reply})
    return reply


# ─── 빠른 커맨드 ─────────────────────────────────────────────────────────────

async def _cmd_status(chat_id: str) -> str:
    """현재 봇 상태 상세 출력"""
    try:
        db    = DB.load_db()
        games = db.get("games", {})
        now   = datetime.now(KST)

        lines = [f"⚾ *봇 상태* `{now.strftime('%m/%d %H:%M')} KST`\n"]
        league_count = {}
        upcoming = []
        for gid, e in games.items():
            lg = e.get("league", "MLB")
            league_count[lg] = league_count.get(lg, 0) + 1
            h = DB.hours_until_game(e)
            if h is not None and 0 <= h <= 6:
                upcoming.append((h, e))

        for lg, cnt in sorted(league_count.items()):
            em = {
                "MLB": "⚾", "KBO": "🇰🇷", "NPB": "🇯🇵",
                "EPL": "🏴󠁧󠁢󠁥󠁮󠁧󠁿", "Bundesliga": "🇩🇪", "SerieA": "🇮🇹", "Ligue1": "🇫🇷",
                "LaLiga": "🇪🇸", "UCL": "🏆", "Championship": "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
            }.get(lg, "🏟")
            lines.append(f"{em} {lg}: {cnt}경기")

        if upcoming:
            lines.append("\n⏰ *6시간 내 경기:*")
            for h, e in sorted(upcoming)[:5]:
                lines.append(f"  {e['away_team']} vs {e['home_team']}  ({h:.1f}h)")

        picks_db = result_checker.load_picks_db()
        picks    = picks_db.get("picks", {})
        pending  = sum(1 for p in picks.values() if p.get("result") is None)
        wins     = sum(1 for p in picks.values() if p.get("result") == "win")
        losses   = sum(1 for p in picks.values() if p.get("result") == "loss")
        total    = wins + losses
        wr_str   = f"{wins}W {losses}L ({100*wins//total}%)" if total else "집계 없음"
        lines.append(f"\n🎯 픽 적중률: {wr_str}")
        lines.append(f"⏳ 결과 대기: {pending}건")
        return "\n".join(lines)
    except Exception as e:
        return f"❌ 상태 조회 오류: {e}"


async def _cmd_picks(chat_id: str) -> str:
    """최근 픽 목록"""
    try:
        picks_db = result_checker.load_picks_db()
        picks    = list(picks_db.get("picks", {}).values())
        if not picks:
            return "📋 저장된 픽이 없습니다."

        # 최신 순으로 최대 5개
        picks.sort(key=lambda p: p.get("sent_at", ""), reverse=True)
        lines = ["🎯 *최근 픽*\n"]
        for p in picks[:5]:
            res = p.get("result")
            res_str = {"win": "✅", "loss": "❌", "push": "↩️", "unknown": "❓"}.get(res, "⏳")
            label = result_checker.pick_label(p)
            lines.append(
                f"{res_str} {p.get('away_team','')} vs {p.get('home_team','')}\n"
                f"   {p.get('market','')} | {label}\n"
                f"   {p.get('sent_at','')[:16]}"
            )
        return "\n\n".join(lines)
    except Exception as e:
        return f"❌ 픽 조회 오류: {e}"


async def _cmd_resetdb(chat_id: str) -> str:
    """경기 DB 초기화 (잘못된 날짜 데이터 등 리셋)"""
    try:
        DB.save_db({"games": {}})
        return "🗑 경기 DB 초기화 완료. 다음 사이클에서 새로 수집합니다."
    except Exception as e:
        return f"❌ 초기화 오류: {e}"


COMMANDS = {
    "/status":  _cmd_status,
    "/picks":   _cmd_picks,
    "/resetdb": _cmd_resetdb,
}


# ─── 메시지 처리 ─────────────────────────────────────────────────────────────

async def _handle_update(update: dict) -> None:
    msg = update.get("message") or update.get("edited_message")
    if not msg:
        return

    chat_id = str(msg["chat"]["id"])
    text    = msg.get("text", "").strip()
    msg_id  = msg["message_id"]

    if not text:
        return
    if chat_id not in ALLOWED:
        logger.warning(f"허용되지 않은 채팅: {chat_id}")
        return

    logger.info(f"[TG] {chat_id}: {text[:80]}")

    # 빠른 커맨드
    cmd = text.split()[0].lower()
    if cmd in COMMANDS:
        reply = await COMMANDS[cmd](chat_id)
    else:
        # Claude 에게 질문
        reply = await _ask_claude(chat_id, text)

    await send_message(chat_id, reply, reply_to=msg_id)


# ─── Long polling 루프 ───────────────────────────────────────────────────────

async def poll_loop() -> None:
    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        logger.info("텔레그램 미설정 — 채팅봇 비활성화")
        return

    logger.info("💬 텔레그램 채팅봇 시작 (long polling)")

    # 기존 업데이트 건너뛰기 (시작 시 이전 메시지 무시)
    resp   = await _tg_get("getUpdates", {"offset": -1, "limit": 1})
    result = resp.get("result", [])
    offset = (result[-1]["update_id"] + 1) if result else 0

    while True:
        try:
            resp    = await _tg_get("getUpdates", {
                "offset":          offset,
                "timeout":         30,
                "allowed_updates": ["message"],
            })
            updates = resp.get("result", [])
            for upd in updates:
                offset = upd["update_id"] + 1
                asyncio.create_task(_handle_update(upd))
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"텔레그램 polling 오류: {e}")
            await asyncio.sleep(5)
