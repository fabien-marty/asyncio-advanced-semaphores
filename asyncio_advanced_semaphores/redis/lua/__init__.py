from pathlib import Path

_SCRIPT_DIR = Path(__file__).parent
RELEASE_SCRIPT = (_SCRIPT_DIR / "release.lua").read_text()
PING_SCRIPT = (_SCRIPT_DIR / "ping.lua").read_text()
QUEUE_SCRIPT = (_SCRIPT_DIR / "queue.lua").read_text()
WAKE_UP_NEXTS_SCRIPT = (_SCRIPT_DIR / "wake_up_nexts.lua").read_text()
ACQUIRE_SCRIPT = (_SCRIPT_DIR / "acquire.lua").read_text()
CARD_SCRIPT = (_SCRIPT_DIR / "card.lua").read_text()
