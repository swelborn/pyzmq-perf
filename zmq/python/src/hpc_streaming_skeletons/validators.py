def is_non_negative(value: int) -> int:
    if value < 0:
        raise ValueError(f"{value} is not a non-negative number")
    return value


def is_positive(value: int) -> int:
    if value <= 0:
        raise ValueError(f"{value} is not a positive number")
    return value
