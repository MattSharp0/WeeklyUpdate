from decimal import Decimal

def _d_round(num: Decimal, places: int):
    return num.quantize(Decimal(f"1.{'0'*places}"), rounding="ROUND_HALF_UP")


def fmt_percentage(number: Decimal, places: int = 0) -> str:
    return f"{_d_round(num=(number*100), places=places)}%"


def fmt_currency(number: Decimal, places: int = 1) -> str:

    if number >= 1_000_000_000:
        div = 1_000_000_000
        char = "B"
    elif number >= 1_000_000:
        div = 1_000_000
        char = "M"
    elif number >= 1_000:
        div = 1_000
        char = "k"
    else:
        div = 1
        char = ""

    return f"${_d_round(num=(number/Decimal(div)), places=places)}{char}"
