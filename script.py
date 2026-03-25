import time
import logging
import random
from functools import wraps
from nba_api.stats.endpoints import playergamelog, leaguegamefinder, commonplayerinfo
from nba_api.stats.static import players

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("nba_ingest.log"),
    ],
)
logger = logging.getLogger("nba_ingest")


# ── Retry decorator ────────────────────────────────────────────────────────────
def retry_with_backoff(max_retries: int = 5, base_delay: float = 1.0, max_delay: float = 60.0):
    """
    Decorator that retries a function with exponential backoff + jitter.
    Retries on any Exception; re-raises after max_retries exhausted.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt <= max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        logger.error(
                            "Max retries (%d) reached for '%s'. Final error: %s",
                            max_retries, func.__name__, e,
                        )
                        raise

                    # Exponential backoff: 1s → 2s → 4s → 8s … capped at max_delay
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    # Add ±25% jitter so parallel workers don't collide
                    jitter = delay * 0.25 * (2 * random.random() - 1)
                    sleep_time = max(0, delay + jitter)

                    logger.warning(
                        "Attempt %d/%d for '%s' failed: %s. Retrying in %.1fs…",
                        attempt + 1, max_retries, func.__name__, e, sleep_time,
                    )
                    time.sleep(sleep_time)
                    attempt += 1
        return wrapper
    return decorator


# ── Fetch helpers ──────────────────────────────────────────────────────────────
@retry_with_backoff(max_retries=5, base_delay=1.0, max_delay=60.0)
def fetch_player_game_log(player_id: int, season: str = "2024-25") -> list[dict]:
    """Return a list of game-log dicts for one player in one season."""
    logger.info("Fetching game log — player_id=%d  season=%s", player_id, season)
    gamelog = playergamelog.PlayerGameLog(
        player_id=player_id,
        season=season,
        timeout=30,         # nba_api passes this to requests
    )
    df = gamelog.get_data_frames()[0]
    logger.info("  → %d rows fetched", len(df))
    return df.to_dict(orient="records")


@retry_with_backoff(max_retries=5, base_delay=1.0, max_delay=60.0)
def fetch_player_info(player_id: int) -> dict:
    """Return bio/meta dict for a single player."""
    logger.info("Fetching player info — player_id=%d", player_id)
    info = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=30)
    df = info.get_data_frames()[0]
    return df.iloc[0].to_dict()


# ── Batch ingest ───────────────────────────────────────────────────────────────
def ingest_players(player_names: list[str], season: str = "2024-25", between_calls: float = 0.6):
    """
    Look up each name, fetch info + game log, return results dict.
    `between_calls` adds a polite delay so you don't hammer the API.
    """
    results = {}

    for name in player_names:
        matches = players.find_players_by_full_name(name)
        if not matches:
            logger.warning("Player not found: '%s' — skipping", name)
            continue

        player = matches[0]
        pid = player["id"]
        logger.info("Processing: %s (id=%d)", player["full_name"], pid)

        try:
            info     = fetch_player_info(pid)
            gamelog  = fetch_player_game_log(pid, season=season)
            results[name] = {"info": info, "games": gamelog}
        except Exception as e:
            logger.error("Failed to ingest '%s' after all retries: %s", name, e)
            results[name] = {"error": str(e)}

        time.sleep(between_calls)   # rate-limit courtesy pause

    return results


# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    targets = ["LeBron James", "Stephen Curry", "Nikola Jokic"]
    data = ingest_players(targets, season="2025-26")

    for player, payload in data.items():
        if "error" in payload:
            print(f"✗ {player}: {payload['error']}")
        else:
            print(f"✓ {player}: {len(payload['games'])} games logged")