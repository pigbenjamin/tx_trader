import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover - optional dependency
    load_dotenv = None

ROOT_DIR = Path(__file__).resolve().parent
API_DLL_PATH = ROOT_DIR / "SKCOM.dll"
LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)


def _load_env_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


if load_dotenv is not None:
    load_dotenv(ROOT_DIR / ".env", override=False)
else:
    _load_env_file(ROOT_DIR / ".env")

# 這裡先預設為微型台灣指數期貨常見商品代碼
DEFAULT_SYMBOLS = ["TX00", "TX01", "TX02"]
DEFAULT_MARKET = "TX"

# 供後續策略使用
DEFAULT_ACCOUNT = os.getenv("TX_TRADE_ACCOUNT", "")
DEFAULT_PASSWORD = os.getenv("TX_TRADE_PASSWORD", "")
