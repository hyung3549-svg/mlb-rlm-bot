import os
from dotenv import load_dotenv

load_dotenv()

# ─── 텔레그램 ──────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID", "")

# ─── Anthropic Claude AI ───────────────────────────
ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY", "")

# ─── 스크래핑 간격 ─────────────────────────────────
SCRAPE_INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL", "300"))  # 5분

# ─── RLM 감지 임계값 ───────────────────────────────
MONEY_THRESHOLD    = float(os.getenv("MONEY_THRESHOLD",    "75.0"))
MIN_LINE_MOVE      = float(os.getenv("MIN_LINE_MOVE",      "0.5"))
MIN_ODDS_MOVE      = float(os.getenv("MIN_ODDS_MOVE",      "10"))

# 오프닝 대비 배당 변화 임계값
ODDS_MOVE_THRESHOLD  = float(os.getenv("ODDS_MOVE_THRESHOLD",  "0.08"))
# 스팀무브 (15분 내) 임계값
STEAM_ODDS_THRESHOLD = float(os.getenv("STEAM_ODDS_THRESHOLD", "0.10"))
# 언오버 라인 이동 임계값
LINE_MOVE_THRESHOLD  = float(os.getenv("LINE_MOVE_THRESHOLD",  "0.5"))
# 스팀무브 라인 이동 임계값
STEAM_LINE_THRESHOLD = float(os.getenv("STEAM_LINE_THRESHOLD", "1.0"))

# ─── 알림 발송 타이밍 ──────────────────────────────
ALERT_WINDOW_MIN = float(os.getenv("ALERT_WINDOW_MIN", "1.0"))   # 1시간 전
ALERT_WINDOW_MAX = float(os.getenv("ALERT_WINDOW_MAX", "3.0"))   # 3시간 전
MONITOR_START    = float(os.getenv("MONITOR_START",   "12.0"))   # 12시간 전부터 수집

# ─── 리그 설정 (BetConstruct Competition ID) ───────
LEAGUES = {
    # 야구
    "MLB": 608,
    "KBO": 611,
    "NPB": 612,
    # 축구
    "EPL":       538,   # 잉글랜드 프리미어리그
    "Bundesliga": 541,  # 독일 분데스리가
    "SerieA":    543,   # 이탈리아 세리에A
    "Ligue1":    548,   # 프랑스 리그1
}

# 리그 → BetConstruct sport alias (스크래퍼 where 절)
LEAGUE_SPORTS = {
    "MLB":       "Baseball",
    "KBO":       "Baseball",
    "NPB":       "Baseball",
    "EPL":       "Soccer",
    "Bundesliga": "Soccer",
    "SerieA":    "Soccer",
    "Ligue1":    "Soccer",
}

# ─── 데이터 저장 경로 ──────────────────────────────
DATA_DIR = os.getenv("DATA_DIR", "./data")

# ─── 로깅 ─────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
