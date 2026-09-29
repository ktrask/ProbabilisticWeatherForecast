"""An in-process cache for upstream answers: time-limited, bounded, and shared.

Open-Meteo updates its models a few times a day and asks for fair use, so a
forecast fetched for a place is kept for an hour. Requests for the same key
that arrive while a fetch is under way wait for that fetch instead of starting
their own - twenty people opening the same link cost one upstream call.
Failures are not cached: the next request tries again.

Each worker process has its own cache. That is fine at this scale; a shared
store (Redis) would be the next step if it stops being fine.
"""
import asyncio
import time
from collections import OrderedDict


class TTLCache:
    def __init__(self, ttl_s, max_entries, clock=time.monotonic):
        self.ttl_s = ttl_s
        self.max_entries = max_entries
        self._clock = clock
        self._entries = OrderedDict()  # key -> (expires, value), least recently used first
        self._pending = {}  # key -> asyncio.Task
        self.hits = 0
        self.misses = 0

    async def get(self, key, factory):
        """The cached value for `key`, or the result of awaiting `factory()`.

        A caller that is cancelled - a client hanging up - does not cancel a
        fetch other callers are waiting for, nor one that would fill the cache.
        """
        entry = self._entries.get(key)
        if entry is not None:
            expires, value = entry
            if expires > self._clock():
                self._entries.move_to_end(key)
                self.hits += 1
                return value
            del self._entries[key]
        self.misses += 1
        task = self._pending.get(key)
        if task is None:
            task = asyncio.get_running_loop().create_task(factory())
            self._pending[key] = task
            task.add_done_callback(lambda done, key=key: self._settle(key, done))
        return await asyncio.shield(task)

    def _settle(self, key, task):
        self._pending.pop(key, None)
        # Asking for the exception also marks it as retrieved, so a failure
        # whose callers were all cancelled is not reported as unhandled.
        if task.cancelled() or task.exception() is not None:
            return
        self._entries[key] = (self._clock() + self.ttl_s, task.result())
        self._entries.move_to_end(key)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    def stats(self):
        return {"entries": len(self._entries), "hits": self.hits, "misses": self.misses}
