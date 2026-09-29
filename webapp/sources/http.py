"""One GET with a timeout, and upstream failures turned into SourceErrors."""
import httpx

from sources.base import SourceError, SourceTimeout


async def get(url, params, *, timeout, what, client=None):
    """GET `url`; any answer but 200 raises. `what` names the service in messages.

    `timeout` (an httpx.Timeout) is applied to this request even on a shared
    `client` configured without one.
    """
    try:
        if client is not None:
            response = await client.get(url, params=params, timeout=timeout)
        else:
            async with httpx.AsyncClient(timeout=timeout) as own:
                response = await own.get(url, params=params)
    except httpx.TimeoutException as exc:
        raise SourceTimeout(f"{what} did not answer within {timeout.read}s") from exc
    except httpx.HTTPError as exc:
        raise SourceError(f"{what} could not be reached: {type(exc).__name__}: {exc}") from exc
    if response.status_code != 200:
        raise SourceError(f"{what} answered {response.status_code}: {_reason(response)}")
    return response


def _reason(response):
    try:
        return response.json().get("reason", response.text[:200])
    except ValueError:
        return response.text[:200]
