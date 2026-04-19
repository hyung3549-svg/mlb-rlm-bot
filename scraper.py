"""
BETWIZ 배당 스크래퍼 (WebSocket 버전)

BetConstruct Swarm WebSocket API 직접 연결 — Playwright 불필요
엔드포인트: wss://eu-swarm-springre.betconstruct.com/

야구 (MLB/KBO/NPB):
  - 승패 (P1P2): W1=홈, W2=어웨이
  - 핸디캡 (RunLine): -1.5 기준
  - 오버/언더 (TotalRunsOver/Under): 1.90 최근접 라인

축구 (EPL 등):
  - 1X2 (P1XP2): W1=홈, X=무, W2=어웨이
  - 핸디캡 (AsianHandicap): 가장 균형잡힌 라인
  - 오버/언더 (OverUnder): 2.5 최근접 라인
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
    """Unix timestamp(UTC) → 'MM/DD HH:MM KST'"""
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(KST)
    return dt.strftime("%m/%d %H:%M") + " KST"


def _safe_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _main_ou_line(ou_rows: list[dict], target: float = 1.90) -> Optional[dict]:
    """오버/언더 라인 중 양쪽 배당이 target에 가장 가까운 메인 라인 반환."""
    best       = None
    best_score = 9999.0
    for row in ou_rows:
        o = row.get("over_odds")
        u = row.get("under_odds")
        if o is None or u is None:
            continue
        score = abs(o - target) + abs(u - target)
        if score < best_score:
            best_score = score
            best       = row
    return best


def _main_hc_line(hc_rows: list[dict]) -> Optional[dict]:
    """핸디캡 라인 중 양쪽 배당이 1.90에 가장 가까운 라인 반환."""
    best       = None
    best_score = 9999.0
    for row in hc_rows:
        fo = row.get("fav_odds")
        do = row.get("dog_odds")
        if fo is None or do is None:
            continue
        score = abs(fo - 1.90) + abs(do - 1.90)
        if score < best_score:
            best_score = score
            best       = row
    return best


# ─── 마켓 파싱 (야구) ────────────────────────────────────────────────────────

def _parse_baseball_markets(markets_data: dict, away_team: str, home_team: str) -> dict:
    """
    야구 마켓 파싱 (MLB/KBO/NPB).
    P1P2 → 승패, RunLine(-1.5) → 핸디캡, TotalRunsOver/Under → 언오버
    """
    moneyline = None
    hc_away: Optional[tuple] = None
    hc_home: Optional[tuple] = None
    ou_map: dict[float, dict] = {}

    for _mid, market in markets_data.items():
        mtype  = market.get("type", "")
        events = market.get("event", {})

        if mtype == "P1P2":
            if moneyline is None:
                away_p = home_p = None
                for ev in events.values():
                    t1    = ev.get("type_1", "")
                    price = _safe_float(ev.get("price"))
                    if t1 == "W2":   away_p = price
                    elif t1 == "W1": home_p = price
                if away_p and home_p:
                    moneyline = {
                        "away_team": away_team, "away_odds": away_p,
                        "home_team": home_team, "home_odds": home_p,
                    }

        elif mtype == "RunLine":
            for ev in events.values():
                base  = _safe_float(ev.get("base"))
                price = _safe_float(ev.get("price"))
                t1    = ev.get("type_1", "")
                if base is None or price is None:
                    continue
                if t1 == "Away":   hc_away = (base, price)
                elif t1 == "Home": hc_home = (base, price)

        elif mtype == "TotalRunsOver/Under":
            mkt_base = _safe_float(market.get("base"))
            for ev in events.values():
                t1      = ev.get("type_1", "")
                price   = _safe_float(ev.get("price"))
                ev_base = _safe_float(ev.get("base"))
                line    = ev_base if ev_base is not None else mkt_base
                if line is None or price is None:
                    continue
                row = ou_map.setdefault(line, {"line": line})
                if t1 == "Over":  row["over_odds"] = price
                elif t1 == "Under": row["under_odds"] = price

    handicap_15 = None
    if hc_away and hc_home:
        away_base, away_price = hc_away
        home_base, home_price = hc_home
        if abs(away_base + 1.5) < 0.01:
            handicap_15 = {
                "fav_team": away_team, "dog_team": home_team,
                "fav_odds": away_price, "dog_odds": home_price,
            }
        elif abs(home_base + 1.5) < 0.01:
            handicap_15 = {
                "fav_team": home_team, "dog_team": away_team,
                "fav_odds": home_price, "dog_odds": away_price,
            }

    return {
        "moneyline":   moneyline,
        "handicap_15": handicap_15,
        "main_ou":     _main_ou_line(list(ou_map.values())),
    }


# ─── 마켓 파싱 (축구) ────────────────────────────────────────────────────────

def _parse_soccer_markets(markets_data: dict, away_team: str, home_team: str) -> dict:
    """
    축구 마켓 파싱 (EPL 등).
    P1XP2 → 1X2 (홈/무/어웨이), AsianHandicap → 균형 라인, OverUnder → 2.5 근접
    moneyline 에 draw_odds 추가로 저장 (3-way 분석용)
    """
    moneyline = None
    hc_rows: list[dict] = []
    ou_map: dict[float, dict] = {}

    for _mid, market in markets_data.items():
        mtype  = market.get("type", "")
        events = market.get("event", {})

        # ── 1X2 ────────────────────────────────────────────────────
        if mtype == "P1XP2":
            if moneyline is None:
                away_p = home_p = draw_p = None
                for ev in events.values():
                    t1    = ev.get("type_1", "")
                    price = _safe_float(ev.get("price"))
                    if t1 == "W2":   away_p = price
                    elif t1 == "W1": home_p = price
                    elif t1 == "X":  draw_p = price
                if away_p and home_p:
                    moneyline = {
                        "away_team": away_team, "away_odds": away_p,
                        "home_team": home_team, "home_odds": home_p,
                        "draw_odds": draw_p,    # 축구 전용 — 무승부
                    }

        # ── 아시안 핸디캡 ───────────────────────────────────────────
        elif mtype == "AsianHandicap":
            mkt_base = _safe_float(market.get("base"))
            home_p = away_p = None
            for ev in events.values():
                t1    = ev.get("type_1", "")
                price = _safe_float(ev.get("price"))
                if t1 == "Home":  home_p = price
                elif t1 == "Away": away_p = price
            if home_p and away_p and mkt_base is not None:
                # fav = base < 0 (불리 팀) → home 이 base<0 이면 home이 페이버릿
                if mkt_base < 0:
                    hc_rows.append({
                        "base": mkt_base,
                        "fav_team": home_team, "dog_team": away_team,
                        "fav_odds": home_p,    "dog_odds": away_p,
                    })
                else:  # base >= 0: away가 불리 (away가 페이버릿)
                    hc_rows.append({
                        "base": mkt_base,
                        "fav_team": away_team, "dog_team": home_team,
                        "fav_odds": away_p,    "dog_odds": home_p,
                    })

        # ── 언오버 ─────────────────────────────────────────────────
        elif mtype == "OverUnder":
            mkt_base = _safe_float(market.get("base"))
            over_p = under_p = None
            for ev in events.values():
                t1    = ev.get("type_1", "")
                price = _safe_float(ev.get("price"))
                if t1 == "Over":  over_p = price
                elif t1 == "Under": under_p = price
            if mkt_base is not None and over_p and under_p:
                ou_map[mkt_base] = {
                    "line": mkt_base,
                    "over_odds": over_p,
                    "under_odds": under_p,
                }

    # 핸디캡: 가장 균형잡힌 라인 선택
    handicap_15 = _main_hc_line(hc_rows)

    # 언오버: 2.5 목표로 가장 가까운 라인 (축구 표준)
    main_ou = _main_ou_line(list(ou_map.values()), target=1.90)

    return {
        "moneyline":   moneyline,
        "handicap_15": handicap_15,
        "main_ou":     main_ou,
    }


# ─── 스포츠별 파싱 분기 ──────────────────────────────────────────────────────

def _parse_markets(markets_data: dict, away_team: str, home_team: str,
                   sport: str = "Baseball") -> dict:
    if sport == "Soccer":
        return _parse_soccer_markets(markets_data, away_team, home_team)
    return _parse_baseball_markets(markets_data, away_team, home_team)


# ─── WebSocket 수집 ─────────────────────────────────────────────────────────

async def _ws_get_league_data(ws, league: str, competition_id: int) -> list[dict]:
    """특정 리그 경기 데이터 요청 (기존 연결된 ws 재사용)."""
    sport = config.LEAGUE_SPORTS.get(league, "Baseball")

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
                "sport":       {"alias": sport},
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
    logger.info(f"[WS] {league}({sport}) 경기 {len(games_raw)}개 수신")

    results = []
    for game_id, game in games_raw.items():
        home_team = game.get("team1_name", "")
        away_team = game.get("team2_name", "")
        start_ts  = game.get("start_ts", 0)
        game_time = _ts_to_kst_hhmm(int(start_ts)) if start_ts else "??:?? KST"

        markets = game.get("market", {})
        parsed  = _parse_markets(markets, away_team, home_team, sport=sport)

        results.append({
            "url":           f"https://www.bwzkix1.com/{competition_id}/{game_id}",
            "match_id":      str(game_id),
            "league":        league,
            "sport":         sport,
            "away_team":     away_team,
            "home_team":     home_team,
            "game_time_kst": game_time,
            "start_ts":      int(start_ts) if start_ts else 0,
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
            max_size=None,               # 무제한 (대형 리그 응답 대응)
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
