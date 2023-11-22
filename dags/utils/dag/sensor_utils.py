import math


POKE_INTERVAL = 60
TIMEOUT = 300


def calculate_delta_time(parent_time_str, child_time_str):
    parent_hour = parent_time_str.split(" ")[1]
    if len(parent_hour.split(",")) > 1:
        parent_hour = int(parent_hour.split(",")[0])
    parent_hour = int(parent_hour)

    parent_minute = parent_time_str.split(" ")[0]
    if len(parent_minute.split(",")) > 1:
        parent_minute = int(parent_minute.split(",")[0])
    parent_minute = int(parent_minute)

    child_hour = child_time_str.split(" ")[1]
    if len(child_hour.split(",")) > 1:
        child_hour = int(child_hour.split(",")[0])
    child_hour = int(child_hour)

    child_minute = child_time_str.split(" ")[0]
    if len(child_minute.split(",")) > 1:
        child_minute = int(child_minute.split(",")[0])
    child_minute = int(child_minute)

    if parent_hour > child_hour:
        child_hour = child_hour + 24
    time1 = parent_hour * 60 + parent_minute
    time2 = child_hour * 60 + child_minute
    total_delta_minute = time2 - time1
    if total_delta_minute < 0:
        return 0, 0
    delta_hour = math.floor(total_delta_minute / 60)
    delta_minute = int(total_delta_minute % 60)
    return delta_hour, delta_minute
