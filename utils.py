"""
<<<<<<< HEAD
Utilidades para obtención de trámites.
=======
Utilidades adicionales para obtención de trámites.
>>>>>>> ca536b5 (Decorator para hacer retry al hacer get tramite)
"""

import asyncio
import functools
import aiohttp


def async_http_retry(max_retries: int = 3, delay: float = 1.0):
    """
    Decorator for async HTTP requests with retry functionality.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds (will exponentially increase)
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            
            for attempt in range(max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    last_exception = e
                    if attempt == max_retries:
                        break
                    
                    print(f"Attempt {attempt + 1} failed: {e}. Retrying in {current_delay}s...")
                    await asyncio.sleep(current_delay)
                    current_delay *= 2  # Exponential backoff
            
            print(f"All {max_retries} retry attempts failed")
            raise last_exception
        return wrapper
    return decorator
