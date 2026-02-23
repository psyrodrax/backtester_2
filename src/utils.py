def clamp(value: float, min_val: float = None, max_val: float = None) -> float:
    if min_val is not None:
        value = max(min_val, value)
    if max_val is not None:
        value = min(max_val, value)
    return value
