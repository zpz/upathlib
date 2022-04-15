import random
import time


class Backoff:
    def __init__(self, base=1, jitter=None, multiplier=2):
        self._base = base
        self._jitter = base if jitter is None else jitter
        self._multiplier = multiplier
        self._time_started = time.perf_counter()
        self.retries = 0

    def sleep(self):
        t = self._base * self._multiplier ** self.retries + random.uniform(0, self._jitter)
        time.sleep(t)
        self.retries += 1

    @property
    def time_elapsed(self):
        return time.perf_counter() - self._time_started  # seconds
