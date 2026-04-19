"""
MLB 배당 역행 감지기 메인

사이클마다:
  1. bwzkix1.com 에서 오늘 MLB 경기 배당 수집
  2. DB 업데이트 (신규 경기는 오프닝 라인 고정)
  3. 신호 분석 (역행 / 스팀무브 / 라인이동 / 신규등록)
  4. 텔레그램 알림 발송
"""

import asyncio
import logging
import sys
from datetime import datetime, timezone, timedelta

import config
import db as DB
import scraper
import analyzer
import notifier
import money_scraper
import result_checker
import telegram_bot

KST = timezone(timedelta(hours=9))

_stdout_handler = logging.StreamHandler(sys.stdout)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        _stdout_handler,
        logging.FileHandler("rlm.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("main")


async def run_once() -> None:
    now_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")
    logger.info(f"━━━ 사이클 시작: {now_str} ━━━")

    # 1. 스크래핑
    games = await scraper.scrape_all()
    logger.info(f"수집 경기 수: {len(games)}")

    if not games:
        logger.warning("수집된 경기 없음")
        return

    # 2. DB 업데이트
    db = DB.load_db()
    new_count = 0
    for game in games:
        entry = DB.update_game(db, game)
        if entry.get("is_new"):
            new_count += 1

    DB.save_db(db)
    logger.info(f"DB 업데이트 완료 (신규: {new_count}개 / 전체: {len(db['games'])}개)")

    # 3. 머니 흐름 수집 ($%)
    money_list = money_scraper.fetch_money_pct()
    logger.info(f"$% 수집: {len(money_list)}경기")

    # 4. 분석
    signals = analyzer.analyze(db, money_list)
    logger.info(f"감지 신호: {len(signals)}건")

    for sig in signals:
        hrs = f"{sig.hours_left:.1f}h" if sig.hours_left is not None else "?"
        logger.info(
            f"  [{sig.signal_type}] {sig.away_team} vs {sig.home_team} "
            f"({hrs} 후) | {sig.market} | {sig.change_val}"
        )

    # 5. 알림
    sent = await notifier.notify(signals)
    logger.info(f"알림 발송: {sent}건")

    # 6. 결과 확인 (경기 종료된 픽 자동 체크)
    try:
        await result_checker.check_results(notifier._send)
    except Exception as e:
        logger.error(f"결과 체크 오류 (무시): {e}")

    logger.info(f"━━━ 사이클 완료 ━━━\n")


async def main_loop() -> None:
    # ── 시작 시 DB 초기화 (날짜 오류 데이터 제거) ─────────────────
    DB.save_db({"games": {}})
    logger.info("🗑 경기 DB 초기화 완료 (start_ts 픽스 적용)")

    logger.info("=" * 60)
    logger.info("  ⚾ MLB 배당 역행 감지기 시작")
    logger.info(f"  수집 간격     : {config.SCRAPE_INTERVAL_SECONDS}초")
    logger.info(f"  배당 역행 임계: {config.MONEY_THRESHOLD}% / {config.MIN_ODDS_MOVE} 배당")
    logger.info(f"  알림 윈도우   : 경기 {analyzer.ALERT_MIN}~{analyzer.ALERT_MAX}시간 전")
    logger.info(f"  스팀무브 임계 : 배당 {analyzer.STEAM_ODDS_THRESHOLD} / 라인 {analyzer.STEAM_LINE_THRESHOLD}")
    logger.info("=" * 60 + "\n")

    while True:
        try:
            await run_once()
        except Exception as e:
            logger.error(f"사이클 오류 (계속 실행): {e}", exc_info=True)

        logger.info(f"{config.SCRAPE_INTERVAL_SECONDS}초 대기 중...\n")
        await asyncio.sleep(config.SCRAPE_INTERVAL_SECONDS)


async def main() -> None:
    """스크래퍼 루프 + 텔레그램 채팅봇 동시 실행"""
    await asyncio.gather(
        main_loop(),
        telegram_bot.poll_loop(),
    )


if __name__ == "__main__":
    asyncio.run(main())
