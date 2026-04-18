"""
sportsbettingdime.com 공개 베팅 비율($%) 수집기

엔드포인트: /wp-json/adpt/v1/mlb-odds
수집 항목: bettingSplits.*.stakePercentage ($% = 실제 돈 기준)
"""

import json
import logging
import urllib.request
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

KST = timezone(timedelta(hours=9))

API_URL = (
    "https://www.sportsbettingdime.com/wp-json/adpt/v1/mlb-odds"
    "?books=sr%3Abook%3A7612%2Csr%3Abook%3A31520%2Csr%3Abook%3A28901%2Csr%3Abook%3A32784"
    "&format=us"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.sportsbettingdime.com/mlb/public-betting-trends/",
}


def _safe_float(v) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _utc_to_kst_hhmm(iso_str: str) -> Optional[str]:
    """ISO UTC 문자열 → 'HH:MM' KST"""
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        kst_dt = dt.astimezone(KST)
        return kst_dt.strftime("%H:%M")
    except Exception:
        return None


def fetch_money_pct() -> list[dict]:
    """
    MLB 전체 경기 $% 데이터 반환.

    각 항목:
        away_abbr, home_abbr, game_time_kst (HH:MM),
        status (not_started / live / closed ...),
        moneyline_home_pct, moneyline_away_pct,
        spread_home_pct, spread_away_pct,
        total_over_pct, total_under_pct
    """
    try:
        req = urllib.request.Request(API_URL, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=15) as resp:
            raw = json.load(resp)
    except Exception as e:
        logger.error(f"[money_scraper] API 요청 실패: {e}")
        return []

    results = []
    for game in raw.get("data", []):
        status = game.get("status", "")
        # 이미 시작됐거나 종료된 경기 제외
        if status not in ("not_started", ""):
            continue
        scheduled = game.get("scheduled", "")
        kst_time = _utc_to_kst_hhmm(scheduled)

        comps = game.get("competitors", {})
        away_abbr = comps.get("away", {}).get("abbreviation", "")
        home_abbr = comps.get("home", {}).get("abbreviation", "")

        splits = game.get("bettingSplits", {})

        ml = splits.get("moneyline", {})
        sp = splits.get("spread", {})
        tot = splits.get("total", {})

        results.append({
            "away_abbr":          away_abbr,
            "home_abbr":          home_abbr,
            "game_time_kst":      kst_time,        # HH:MM
            "status":             status,
            "moneyline_home_pct": _safe_float(ml.get("home", {}).get("stakePercentage")),
            "moneyline_away_pct": _safe_float(ml.get("away", {}).get("stakePercentage")),
            "spread_home_pct":    _safe_float(sp.get("home", {}).get("stakePercentage")),
            "spread_away_pct":    _safe_float(sp.get("away", {}).get("stakePercentage")),
            "total_over_pct":     _safe_float(tot.get("over",  {}).get("stakePercentage")),
            "total_under_pct":    _safe_float(tot.get("under", {}).get("stakePercentage")),
        })

    logger.info(f"[money_scraper] {len(results)}경기 $% 수집 완료")
    return results


def match_money_to_game(money_list: list[dict], game_time_kst: str) -> Optional[dict]:
    """
    game_time_kst ('HH:MM KST' 또는 'HH:MM') 기준으로 money_list에서 해당 경기 매칭.
    """
    t = game_time_kst.replace(" KST", "").strip()
    for m in money_list:
        if m.get("game_time_kst") == t:
            return m
    return None


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    data = fetch_money_pct()
    for d in data:
        ml_h = d["moneyline_home_pct"]
        ml_a = d["moneyline_away_pct"]
        print(
            f"{d['away_abbr']} @ {d['home_abbr']}  {d['game_time_kst']} KST"
            f"  ML $%: away={ml_a}% home={ml_h}%"
            f"  O/U $%: over={d['total_over_pct']}% under={d['total_under_pct']}%"
            f"  [{d['status']}]"
        )
