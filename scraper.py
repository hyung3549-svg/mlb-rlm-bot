"""
BETWIZ MLB 배당 스크래퍼 (WebSocket 버전)

BetConstruct Swarm WebSocket API 직접 연결 — Playwright 불필요
엔드포인트: wss://eu-swarm-springre.betconstruct.com/

수집 항목:
  - 승패 (P1P2 / MoneyLine): 홈/원정 배당
  - 핸디캡 1.5 (RunLine): -1.5 / +1.5 배당
  - 오버/언더 (TotalRuns*): 1.90에 가장 가까운 메인 라인
"""

import asyncio
import json
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Optional

import websockets

import config

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

WS_URL  = "wss://eu-swarm-springre.betconstruct.com/"
SITE_ID = "18747716"
AFEC    = "dcnwYYDt8VI9EMDflnoY8j-qw919zRj47uOK"


# ─── 유틸리티 ───────────────────────────────────────────────────────────────

def _kst_now() -> str:
    return datetime.now(KST).strftime("%Y-%m-%d %H:%M KST")


def _ts_to_kst_hhmm(ts: int) -> str:
    """Unix timestamp(UTC) → 'HH:MM KST'"""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(KST)
    return dt.strftime("%H:%M") + " KST"


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _main_ou_line(ou_rows: list[dict]) -> Optional[dict]:
    """오버/언더 라인 중 양쪽 배당이 1.90에 가장 가까운 메인 라인 반환."""
    best       = None
    best_score = 9999.0
    for row in ou_rows:
        o = row.get("over_odds")
        u = row.get("under_odds")
        if o is None or u is None:
            continue
        score = abs(o - 1.90) + abs(u - 1.90)
        if score < best_score:
            best_score = score
            best       = row
    return best


# ─── 마켓 파싱 ───────────────────────────────────────────────────────────────

def _parse_markets(markets_data: dict, away_team: str, home_team: str) -> dict:
    """
    게임에 딸린 마켓 딕셔너리에서 세 가지 항목 추출.

    BetConstruct market.type 값:
      - 'P1P2' 또는 'MoneyLine' → 승패
      - 'RunLine'               → 핸디캡 (base=-1.5 가 페이버릿)
      - 'TotalRuns' / 'TotalRunsOver' / 'TotalRunsUnder' → 오버언더

    event.type_1: 'Away' | 'Home' | 'Over' | 'Under'
    event.base  : 해당 이벤트의 핸디/라인 수치
    """
    moneyline   = None
    hc_away: Optional[tuple] = None   # (base, price)
    hc_home: Optional[tuple] = None
    ou_map: dict[float, dict] = {}    # line → {over_odds, under_odds}

    for _mid, market in markets_data.items():
        mtype  = market.get("type", "")
        mname  = market.get("name", "")
        events = market.get("event", {})

        # ── 승패 (P1P2) ─────────────────────────────────────────────
        # BetConstruct: W1=홈(team1), W2=어웨이(team2)
        if mtype == "P1P2":
            if moneyline is None:
                away_p = home_p = None
                for ev in events.values():
                    t1    = ev.get("type_1", "")
                    price = _safe_float(ev.get("price"))
                    if t1 == "W2":        # Away
                        away_p = price
                    elif t1 == "W1":      # Home
                        home_p = price
                if away_p and home_p:
                    moneyline = {
                        "away_team": away_team,
                        "home_team": home_team,
                        "away_odds": away_p,
                        "home_odds": home_p,
                    }

        # ── 핸디캡 (RunLine) ────────────────────────────────────────
        elif mtype == "RunLine":
            for ev in events.values():
                base  = _safe_float(ev.get("base"))
                price = _safe_float(ev.get("price"))
                t1    = ev.get("type_1", "")
                if base is None or price is None:
                    continue
                if t1 == "Away":
                    hc_away = (base, price)
                elif t1 == "Home":
                    hc_home = (base, price)

        # ── 오버/언더 (메인 게임 전체, 이닝/5이닝 제외) ──────────────
        elif mtype == "TotalRunsOver/Under":
            # market.base 에 라인 값이 있을 수도, event.base 에 있을 수도 있음
            mkt_base = _safe_float(market.get("base"))
            for ev in events.values():
                t1       = ev.get("type_1", "")
                price    = _safe_float(ev.get("price"))
                ev_base  = _safe_float(ev.get("base"))
                line     = ev_base if ev_base is not None else mkt_base
                if line is None or price is None:
                    continue
                row = ou_map.setdefault(line, {})
                if t1 == "Over":
                    row["over_odds"]   = price
                    row["over_label"]  = f"오버 ({line})"
                    row["line"]        = line
                elif t1 == "Under":
                    row["under_odds"]  = price
                    row["under_label"] = f"언더 ({line})"
                    row["line"]        = line

    # ── 핸디캡 조립 ──────────────────────────────────────────────────
    handicap_15 = None
    if hc_away and hc_home:
        away_base, away_price = hc_away
        home_base, home_price = hc_home
        if abs(away_base + 1.5) < 0.01:          # 어웨이가 -1.5 (페이버릿)
            handicap_15 = {
                "fav_team": away_team,
                "dog_team": home_team,
                "fav_odds": away_price,
                "dog_odds": home_price,
            }
        elif abs(home_base + 1.5) < 0.01:        # 홈이 -1.5 (페이버릿)
            handicap_15 = {
                "fav_team": home_team,
                "dog_team": away_team,
                "fav_odds": home_price,
                "dog_odds": away_price,
            }

    # ── 메인 O/U 선택 ────────────────────────────────────────────────
    main_ou = _main_ou_line(list(ou_map.values()))

    return {
        "moneyline":   moneyline,
        "handicap_15": handicap_15,
        "main_ou":     main_ou,
    }


# ─── WebSocket 수집 ─────────────────────────────────────────────────────────

async def _ws_get_league_data(ws, league: str, competition_id: int) -> list[dict]:
    """특정 리그 경기 데이터 요청 (기존 연결된 ws 재사용)."""
    await ws.send(json.dumps({
        "command": "get",
        "params": {
            "source": "betting",
            "what": {
                "game":   ["id", "team1_name", "team2_name",
                           "start_ts", "markets_count", "is_started"],
                "market": ["id", "name", "type", "base"],
                "event":  ["id", "name", "price", "type_1", "base"],
            },
            "where": {
                "sport":       {"alias": "Baseball"},
                "competition": {"id": competition_id},
                "game":        {"is_started": 0},
            },
            "subscribe": False,
        },
    }))

    raw  = await asyncio.wait_for(ws.recv(), timeout=20)
    resp = json.loads(raw)

    games_raw = (
        resp.get("data", {})
            .get("data", {})
            .get("game", {})
    )
    logger.info(f"[WS] {league} 경기 {len(games_raw)}개 수신")

    results = []
    for game_id, game in games_raw.items():
        home_team = game.get("team1_name", "")
        away_team = game.get("team2_name", "")
        start_ts  = game.get("start_ts", 0)
        game_time = _ts_to_kst_hhmm(int(start_ts)) if start_ts else "??:?? KST"

        markets = game.get("market", {})
        parsed  = _parse_markets(markets, away_team, home_team)

        results.append({
            "url":           f"https://www.bwzkix1.com/{competition_id}/{game_id}",
            "match_id":      str(game_id),
            "league":        league,
            "away_team":     away_team,
            "home_team":     home_team,
            "game_time_kst": game_time,
            "fetched_at":    _kst_now(),
            "moneyline":     parsed["moneyline"],
            "handicap_15":   parsed["handicap_15"],
            "main_ou":       parsed["main_ou"],
        })
    return results


async def _ws_scrape_all() -> list[dict]:
    """모든 리그 데이터를 하나의 WebSocket 연결로 수집."""
    all_results = []

    try:
        async with websockets.connect(
            WS_URL,
            additional_headers={
                "Origin":     "https://www.bwzkix1.com",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/123.0.0.0 Safari/537.36"
                ),
            },
            ping_interval=20,
            ping_timeout=10,
        ) as ws:

            # ── 세션 인증 ─────────────────────────────────────────
            await ws.send(json.dumps({
                "command": "request_session",
                "params": {
                    "site_id":  SITE_ID,
                    "language": "kor",
                    "source":   "betting",
                    "afec":     AFEC,
                },
            }))

            raw  = await asyncio.wait_for(ws.recv(), timeout=10)
            sess = json.loads(raw)
            sid  = sess.get("data", {}).get("sid")
            if not sid:
                logger.error(f"[WS] 세션 실패: {sess}")
                return []
            logger.info(f"[WS] 세션 연결 (sid={sid})")

            # ── 리그별 순차 요청 ──────────────────────────────────
            for league, comp_id in config.LEAGUES.items():
                try:
                    games = await _ws_get_league_data(ws, league, comp_id)
                    all_results.extend(games)
                except asyncio.TimeoutError:
                    logger.warning(f"[WS] {league} 타임아웃, 건너뜀")
                except Exception as e:
                    logger.error(f"[WS] {league} 오류: {e}")

    except websockets.exceptions.WebSocketException as e:
        logger.error(f"[WS] WebSocket 오류: {e}")
    except Exception as e:
        logger.error(f"[WS] 예외: {e}", exc_info=True)

    return all_results


# ─── 메인 진입점 ─────────────────────────────────────────────────────────────

async def scrape_all() -> list[dict]:
    os.makedirs(config.DATA_DIR, exist_ok=True)

    results = await _ws_scrape_all()

    if not results:
        logger.warning("수집된 경기 없음")
        return []

    out_path = os.path.join(config.DATA_DIR, "latest.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    logger.info(f"총 {len(results)}경기 저장 → {out_path}")
    return results


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    asyncio.run(scrape_all())
