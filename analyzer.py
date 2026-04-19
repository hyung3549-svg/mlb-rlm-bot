"""
배당 역행 분석 엔진

RLM 정의:
  공개 베팅 $% ≥ 75% 한쪽 쏠림
  + 그 쪽 배당이 오프닝 대비 악화(decimal 기준 상승)
  = 샤프머니가 반대쪽에 → 진짜 역행

신호 종류:
  NEW_GAME  - 신규 경기 등록 즉시
  STEAM     - 15분 내 급격한 배당 변화 즉시
  RLM       - 경기 1~3시간 전, $% 확인된 역행
  LINE_RLM  - 언오버 라인 자체가 $% 반대 방향으로 이동
  FINAL     - 경기 30~45분 전 최종 스냅샷

$% 등급:
  65~74%  : 관심 👀
  75~84%  : 무거움 ⚠️  (RLM 트리거)
  85%+    : 극단  🔥
"""

from dataclasses import dataclass
from typing import Optional
import config
import db as DB

# ─── 임계값 ─────────────────────────────────────────────────────────────────
ODDS_MOVE_THRESHOLD  = float(getattr(config, "ODDS_MOVE_THRESHOLD",  0.08))
STEAM_ODDS_THRESHOLD = float(getattr(config, "STEAM_ODDS_THRESHOLD", 0.10))
STEAM_ODDS_MAX       = float(getattr(config, "STEAM_ODDS_MAX",       1.0))   # 이 이상은 데이터 오류
STEAM_LINE_THRESHOLD = float(getattr(config, "STEAM_LINE_THRESHOLD", 1.0))
LINE_MOVE_THRESHOLD  = float(getattr(config, "LINE_MOVE_THRESHOLD",  0.5))

ALERT_MIN   = float(getattr(config, "ALERT_WINDOW_MIN", 1.0))   # 1h
ALERT_MAX   = float(getattr(config, "ALERT_WINDOW_MAX", 3.0))   # 3h
FINAL_MIN   = 0.50   # 30분
FINAL_MAX   = 0.75   # 45분
MONITOR_MAX = float(getattr(config, "MONITOR_START", 12.0))     # 12h

STEAM_HISTORY_COUNT = 3   # 최근 3스냅샷 ≈ 15분

MONEY_TIER_MIN      = 65.0   # 관심 시작
MONEY_RLM_THRESHOLD = 75.0   # RLM 트리거
MONEY_EXTREME       = 85.0   # 극단


# ─── 데이터 클래스 ───────────────────────────────────────────────────────────

@dataclass
class Signal:
    signal_type: str          # NEW_GAME / STEAM / RLM / LINE_RLM / FINAL
    match_id:    str
    away_team:   str
    home_team:   str
    game_time:   str
    hours_left:  Optional[float]
    market:      str
    description: str
    opening_val: str = ""
    current_val: str = ""
    change_val:  str = ""
    money_pct:   str = ""     # 등급 포함 문자열
    url:         str = ""
    league:      str = "MLB"
    pick_side:   str = ""     # away/home/over/under/fav/dog  (픽 방향)


# ─── 유틸리티 ────────────────────────────────────────────────────────────────

def _f(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _money_tier(pct: Optional[float]) -> str:
    """$% 값 → 등급 문자열"""
    if pct is None:
        return ""
    if pct >= MONEY_EXTREME:
        return f"🔥 극단 {pct}%"
    if pct >= MONEY_RLM_THRESHOLD:
        return f"⚠️ 무거움 {pct}%"
    if pct >= MONEY_TIER_MIN:
        return f"👀 관심 {pct}%"
    return f"{pct}%"


def _is_rlm(side_pct: Optional[float], odds_diff: Optional[float]) -> bool:
    """
    RLM 확인:
    - 돈이 ≥ 75% 몰린 쪽
    - 해당 쪽 decimal 배당이 오프닝 대비 상승(악화)
    → 북메이커가 공개 베팅 반대로 라인 이동 = 샤프머니 존재
    """
    return (
        side_pct is not None and side_pct >= MONEY_RLM_THRESHOLD
        and odds_diff is not None
        and ODDS_MOVE_THRESHOLD < odds_diff <= STEAM_ODDS_MAX
    )


# ─── 마켓별 비교 ─────────────────────────────────────────────────────────────

def _compare_ml(open_ml: dict, curr_ml: dict) -> list[dict]:
    if not open_ml or not curr_ml:
        return []
    result = []
    for side in ("away", "home"):
        o = _f(open_ml.get(f"{side}_odds"))
        c = _f(curr_ml.get(f"{side}_odds"))
        if o is None or c is None:
            continue
        result.append({
            "side":  side,
            "team":  open_ml.get(f"{side}_team", side),
            "open":  o,
            "curr":  c,
            "diff":  round(c - o, 4),
        })
    return result


def _compare_hc(open_hc: dict, curr_hc: dict) -> list[dict]:
    if not open_hc or not curr_hc:
        return []
    result = []
    for key, label_fn in [
        ("fav_odds", lambda h: f"{h.get('fav_team','정배')} (-1.5)"),
        ("dog_odds", lambda h: f"{h.get('dog_team','역배')} (+1.5)"),
    ]:
        o = _f(open_hc.get(key))
        c = _f(curr_hc.get(key))
        if o is None or c is None:
            continue
        result.append({
            "key":   key,
            "label": label_fn(open_hc),
            "open":  o,
            "curr":  c,
            "diff":  round(c - o, 4),
        })
    return result


def _compare_ou(open_ou: dict, curr_ou: dict) -> dict:
    if not open_ou or not curr_ou:
        return {}
    ol = _f(open_ou.get("line"));  cl = _f(curr_ou.get("line"))
    oo = _f(open_ou.get("over_odds"));  co = _f(curr_ou.get("over_odds"))
    ou = _f(open_ou.get("under_odds")); cu = _f(curr_ou.get("under_odds"))
    return {
        "line_open":  ol, "line_curr":  cl,
        "line_diff":  round(cl - ol, 4) if (ol is not None and cl is not None) else None,
        "over_open":  oo, "over_curr":  co,
        "over_diff":  round(co - oo, 4) if (oo and co) else None,
        "under_open": ou, "under_curr": cu,
        "under_diff": round(cu - ou, 4) if (ou and cu) else None,
    }


# ─── 신호 생성 ────────────────────────────────────────────────────────────────

def _base(entry: dict, hours: Optional[float], sig_type: str) -> dict:
    return dict(
        signal_type = sig_type,
        match_id    = entry.get("match_id", ""),
        away_team   = entry["away_team"],
        home_team   = entry["home_team"],
        game_time   = entry["game_time_kst"],
        hours_left  = hours,
        url         = entry.get("url", ""),
        league      = entry.get("league", "MLB"),
    )


def _rlm_moneyline(entry: dict, hours: Optional[float],
                   money: Optional[dict], sig_type: str = "RLM") -> list[Signal]:
    signals = []
    op = entry["opening"].get("moneyline")
    cu = entry["current"].get("moneyline")

    away_pct = money.get("moneyline_away_pct") if money else None
    home_pct = money.get("moneyline_home_pct") if money else None

    for ch in _compare_ml(op, cu):
        side_pct = away_pct if ch["side"] == "away" else home_pct
        opp_pct  = home_pct if ch["side"] == "away" else away_pct

        if not _is_rlm(side_pct, ch["diff"]):
            continue

        # 반대 쪽 $% 도 표시
        money_str = (
            f"{_money_tier(side_pct)} → {ch['team']} 배당 악화  "
            f"(상대 {_money_tier(opp_pct)})"
        )
        # 배당역행: ch["side"]에 돈 몰렸는데 배당 악화 → 샤프머니는 반대쪽 → 픽: 반대
        pick = "home" if ch["side"] == "away" else "away"
        signals.append(Signal(
            **_base(entry, hours, sig_type),
            market      = "승패",
            description = f"$% {side_pct}% 쏠린 {ch['team']} 배당 오프닝 대비 상승(악화) ↑",
            opening_val = str(ch["open"]),
            current_val = str(ch["curr"]),
            change_val  = f"{ch['diff']:+.2f}",
            money_pct   = money_str,
            pick_side   = pick,
        ))
    return signals


def _rlm_hc(entry: dict, hours: Optional[float],
            money: Optional[dict], sig_type: str = "RLM") -> list[Signal]:
    signals = []
    op = entry["opening"].get("handicap_15")
    cu = entry["current"].get("handicap_15")

    # 핸디캡 $%: spread 사용
    sp_away = money.get("spread_away_pct") if money else None
    sp_home = money.get("spread_home_pct") if money else None

    for ch in _compare_hc(op, cu):
        # fav_odds → fav(홈) 쪽 $%, dog_odds → away 쪽 $%  (근사치로 ML 사용)
        # spread $%가 없으면 ML $%로 대체
        if ch["key"] == "fav_odds":
            side_pct = sp_home or (money.get("moneyline_home_pct") if money else None)
        else:
            side_pct = sp_away or (money.get("moneyline_away_pct") if money else None)

        if not _is_rlm(side_pct, ch["diff"]):
            continue

        # 배당역행: ch["key"] 쪽에 돈 몰렸는데 배당 악화 → 픽: 반대
        pick = "dog" if ch["key"] == "fav_odds" else "fav"
        signals.append(Signal(
            **_base(entry, hours, sig_type),
            market      = f"핸디캡 {ch['label']}",
            description = f"$% {side_pct}% 쏠린 {ch['label']} 배당 악화 ↑",
            opening_val = str(ch["open"]),
            current_val = str(ch["curr"]),
            change_val  = f"{ch['diff']:+.2f}",
            money_pct   = _money_tier(side_pct),
            pick_side   = pick,
        ))
    return signals


def _rlm_ou(entry: dict, hours: Optional[float],
            money: Optional[dict], sig_type: str = "RLM") -> list[Signal]:
    signals = []
    op = entry["opening"].get("main_ou")
    cu = entry["current"].get("main_ou")
    cmp = _compare_ou(op, cu)
    if not cmp:
        return signals

    over_pct  = money.get("total_over_pct")  if money else None
    under_pct = money.get("total_under_pct") if money else None

    # 1) 라인 숫자 이동 — $% 반대 방향이면 LINE_RLM
    ld = cmp.get("line_diff")
    if ld is not None and abs(ld) >= LINE_MOVE_THRESHOLD:
        # 오버에 돈 몰렸는데 라인 하락 = 언더로 돌아감 = 역행
        is_line_rlm = (
            (over_pct  and over_pct  >= MONEY_RLM_THRESHOLD and ld < 0) or
            (under_pct and under_pct >= MONEY_RLM_THRESHOLD and ld > 0)
        )
        heavy_side = (
            f"오버 {_money_tier(over_pct)}" if (over_pct and over_pct >= MONEY_RLM_THRESHOLD and ld < 0)
            else f"언더 {_money_tier(under_pct)}"
        )
        direction = "하락 ↓" if ld < 0 else "상승 ↑"
        _ltype = "LINE_RLM" if is_line_rlm else "LINE_MOVE"
        # LINE_RLM: 오버에 돈 + 라인 하락 → 언더 픽  /  언더에 돈 + 라인 상승 → 오버 픽
        line_pick = ("under" if ld < 0 else "over") if is_line_rlm else ""
        signals.append(Signal(
            **_base(entry, hours, _ltype),
            market      = "언오버 기준점",
            description = (
                f"라인 {direction}  ({heavy_side} 베팅 역방향)"
                if is_line_rlm else f"U/O 기준점 이동 {direction}"
            ),
            opening_val = str(cmp["line_open"]),
            current_val = str(cmp["line_curr"]),
            change_val  = f"{ld:+.1f}",
            money_pct   = f"$% 오버 {over_pct}% / 언더 {under_pct}%" if (over_pct or under_pct) else "",
            pick_side   = line_pick,
        ))

    # 2) 오버 배당 악화 (오버에 돈 몰렸는데 배당 상승) → 언더 픽
    od = cmp.get("over_diff")
    if od is not None and _is_rlm(over_pct, od):
        signals.append(Signal(
            **_base(entry, hours, sig_type),
            market      = f"언오버 오버({cmp['line_curr']})",
            description = f"$% {over_pct}% 오버 쏠림, 오버 배당 악화 ↑",
            opening_val = str(cmp["over_open"]),
            current_val = str(cmp["over_curr"]),
            change_val  = f"{od:+.2f}",
            money_pct   = f"오버 {_money_tier(over_pct)} / 언더 {_money_tier(under_pct)}",
            pick_side   = "under",
        ))

    # 3) 언더 배당 악화 (언더에 돈 몰렸는데 배당 상승) → 오버 픽
    ud = cmp.get("under_diff")
    if ud is not None and _is_rlm(under_pct, ud):
        signals.append(Signal(
            **_base(entry, hours, sig_type),
            market      = f"언오버 언더({cmp['line_curr']})",
            description = f"$% {under_pct}% 언더 쏠림, 언더 배당 악화 ↑",
            opening_val = str(cmp["under_open"]),
            current_val = str(cmp["under_curr"]),
            change_val  = f"{ud:+.2f}",
            money_pct   = f"오버 {_money_tier(over_pct)} / 언더 {_money_tier(under_pct)}",
            pick_side   = "over",
        ))

    return signals


def _steam(entry: dict, hours: Optional[float]) -> list[Signal]:
    history = entry.get("history", [])
    if len(history) < STEAM_HISTORY_COUNT:
        return []
    old = history[-STEAM_HISTORY_COUNT]
    new = history[-1]
    signals = []
    b = _base(entry, hours, "STEAM")

    # 승패 스팀 — 양쪽 동시 변동은 하나로 합침
    # STEAM_ODDS_MAX 초과는 팀 순서 뒤집힘 등 데이터 오류로 필터
    ml_changes = [ch for ch in _compare_ml(old.get("moneyline"), new.get("moneyline"))
                  if STEAM_ODDS_THRESHOLD <= abs(ch["diff"]) <= STEAM_ODDS_MAX]
    if ml_changes:
        ml_drops = [ch for ch in ml_changes if ch["diff"] < 0]
        ml_rises = [ch for ch in ml_changes if ch["diff"] > 0]

        if ml_drops and ml_rises:
            # 양쪽 다 임계치 초과: 급락팀 기준 표시
            drop = max(ml_drops, key=lambda c: abs(c["diff"]))
            rise = max(ml_rises, key=lambda c: abs(c["diff"]))
            ref  = drop
            desc = f"{drop['team']} 배당 급락 ❄️  /  {rise['team']} 배당 급등 🔥"
            ml_pick = drop["side"]
        elif ml_drops:
            # 한 팀만 급락
            drop = ml_drops[0]
            ref  = drop
            desc = f"{drop['team']} 배당 급락 ❄️"
            ml_pick = drop["side"]
        else:
            # 한 팀만 급등 → 반대 팀에 샤프머니 유입된 것
            rise = ml_rises[0]
            ref  = rise
            desc = f"{rise['team']} 배당 급등 🔥  (반대 팀 샤프 유입)"
            ml_pick = "home" if rise["side"] == "away" else "away"

        signals.append(Signal(**b,
            market      = "승패 [스팀]",
            description = desc,
            opening_val = str(ref["open"]),
            current_val = str(ref["curr"]),
            change_val  = f"{ref['diff']:+.2f}",
            pick_side   = ml_pick,
        ))

    # 핸디캡 스팀 — 동일하게 급락팀 기준 하나로
    hc_changes = [ch for ch in _compare_hc(old.get("handicap_15"), new.get("handicap_15"))
                  if STEAM_ODDS_THRESHOLD <= abs(ch["diff"]) <= STEAM_ODDS_MAX]
    if hc_changes:
        hc_drops = [ch for ch in hc_changes if ch["diff"] < 0]
        hc_rises = [ch for ch in hc_changes if ch["diff"] > 0]

        if hc_drops and hc_rises:
            drop = max(hc_drops, key=lambda c: abs(c["diff"]))
            rise = max(hc_rises, key=lambda c: abs(c["diff"]))
            ref  = drop
            desc = f"{drop['label']} 배당 급락 ❄️  /  {rise['label']} 배당 급등 🔥"
            hc_pick = "fav" if drop["key"] == "fav_odds" else "dog"
        elif hc_drops:
            drop = hc_drops[0]
            ref  = drop
            desc = f"{drop['label']} 배당 급락 ❄️"
            hc_pick = "fav" if drop["key"] == "fav_odds" else "dog"
        else:
            rise = hc_rises[0]
            ref  = rise
            desc = f"{rise['label']} 배당 급등 🔥  (반대 팀 샤프 유입)"
            hc_pick = "dog" if rise["key"] == "fav_odds" else "fav"

        signals.append(Signal(**b,
            market      = f"핸디캡 [스팀]",
            description = desc,
            opening_val = str(ref["open"]),
            current_val = str(ref["curr"]),
            change_val  = f"{ref['diff']:+.2f}",
            pick_side   = hc_pick,
        ))

    # 언오버 라인 스팀
    cmp = _compare_ou(old.get("main_ou"), new.get("main_ou"))
    ld = cmp.get("line_diff")
    if ld is not None and abs(ld) >= STEAM_LINE_THRESHOLD:
        d = "급상승 🔥" if ld > 0 else "급하락 ❄️"
        # 라인 급등 → 샤프가 오버 베팅 → 오버 픽 / 라인 급락 → 샤프가 언더 베팅 → 언더 픽
        ou_pick = "over" if ld > 0 else "under"
        signals.append(Signal(**b,
            market      = "언오버 기준점 [스팀]",
            description = f"U/O 기준점 15분 내 {d}",
            opening_val = str(cmp["line_open"]),
            current_val = str(cmp["line_curr"]),
            change_val  = f"{ld:+.1f}",
            pick_side   = ou_pick,
        ))

    return signals


# ─── 메인 분석 ───────────────────────────────────────────────────────────────

def analyze(db: dict, money_list: Optional[list] = None) -> list[Signal]:
    """
    DB 전체 순회 → 신호 생성.
    money_list: money_scraper.fetch_money_pct() 결과 (없으면 None)
    """
    from money_scraper import match_money_to_game
    signals = []

    for gid, entry in db.get("games", {}).items():
        entry["match_id"] = gid
        hours = DB.hours_until_game(entry)

        # 12시간 이상 남은 경기 스킵
        if hours is not None and hours > MONITOR_MAX:
            continue

        # 이미 시작된 경기 스킵 (hours < 0 이거나 status 확인)
        if hours is not None and hours < 0:
            continue

        money = match_money_to_game(money_list or [], entry.get("game_time_kst", ""))

        # ── 신규 경기 ──────────────────────────────────────────────
        if entry.get("is_new"):
            ml = entry["opening"].get("moneyline") or {}
            hc = entry["opening"].get("handicap_15") or {}
            ou = entry["opening"].get("main_ou") or {}
            signals.append(Signal(
                signal_type = "NEW_GAME",
                match_id    = gid,
                away_team   = entry["away_team"],
                home_team   = entry["home_team"],
                game_time   = entry["game_time_kst"],
                hours_left  = hours,
                market      = "신규 등록",
                description = (
                    f"승패 어웨이 {ml.get('away_odds','?')} / 홈 {ml.get('home_odds','?')}  |  "
                    f"핸디(-1.5) {hc.get('fav_odds','?')}  |  "
                    f"U/O {ou.get('line','?')} "
                    f"오버 {ou.get('over_odds','?')} / 언더 {ou.get('under_odds','?')}"
                ),
                opening_val = "오프닝 라인 고정",
                url         = entry.get("url", ""),
                league      = entry.get("league", "MLB"),
            ))
            continue

        # ── 스팀무브 (즉시, 시간 무관) ─────────────────────────────
        signals.extend(_steam(entry, hours))

        # ── RLM (경기 1~3시간 전) ──────────────────────────────────
        if hours is not None and ALERT_MIN <= hours <= ALERT_MAX:
            signals.extend(_rlm_moneyline(entry, hours, money))
            signals.extend(_rlm_hc(entry, hours, money))
            signals.extend(_rlm_ou(entry, hours, money))

        # ── 최종 스냅샷 (경기 30~45분 전) ─────────────────────────
        if hours is not None and FINAL_MIN <= hours <= FINAL_MAX:
            finals = (
                _rlm_moneyline(entry, hours, money, sig_type="FINAL") +
                _rlm_hc(entry, hours, money, sig_type="FINAL") +
                _rlm_ou(entry, hours, money, sig_type="FINAL")
            )
            signals.extend(finals)

    return signals
