import time
from functools import wraps


def retry_on_exception(
    max_attempts=3,
    delay_seconds=2,
    backoff_factor=2,
    allowed_exceptions=(Exception,),
):
    """
    Retry decorator with exponential backoff.

    Example:
    attempt 1 -> wait 2 sec
    attempt 2 -> wait 4 sec
    attempt 3 -> fail
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 1
            current_delay = delay_seconds

            while attempt <= max_attempts:
                try:
                    return func(*args, **kwargs)
                except allowed_exceptions as e:
                    if attempt == max_attempts:
                        raise

                    print(
                        f"[Retry] {func.__name__} failed on attempt "
                        f"{attempt}/{max_attempts}: {e}. "
                        f"Retrying in {current_delay} seconds..."
                    )
                    time.sleep(current_delay)
                    current_delay *= backoff_factor
                    attempt += 1

        return wrapper

    return decorator