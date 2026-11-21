"""Stop capture when the downstream pipeline falls persistently behind."""

from collections import Counter

STAGES = ("processor", "archive", "frames", "raw_clickhouse")


class Overload(RuntimeError):
    pass


class LoadGuard:
    def __init__(self, limit=30, grace=90, consecutive=3):
        if limit < 10:
            raise ValueError("MAX_PIPELINE_LAG_SECONDS must be at least 10")
        self.limit = limit
        self.grace = grace
        self.consecutive = consecutive
        self.latest = {}
        self.exceeded = Counter()
        self.start = None

    def progress(self, stage, second):
        self.latest[stage] = max(second, self.latest.get(stage, second))

    def sample(self, second):
        if self.start is None:
            self.start = second
        lag = {stage: max(0, second - self.latest.get(stage, self.start)) for stage in STAGES}
        if second - self.start >= self.grace:
            for stage, seconds in lag.items():
                self.exceeded[stage] = self.exceeded[stage] + 1 if seconds > self.limit else 0
                if self.exceeded[stage] >= self.consecutive:
                    raise Overload(f"{stage} lag {seconds}s exceeds {self.limit}s for "
                                   f"{self.consecutive} consecutive samples")
        return lag
