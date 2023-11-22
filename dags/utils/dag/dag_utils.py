CONCURRENCY = 8
MAX_ACTIVE_RUNS = 2


class DagSchedulerType:
    # daily ex. n-1, n-2...
    DAILY = "daily"

    # in_day ex. in day n
    IN_DAY = "in_day"
