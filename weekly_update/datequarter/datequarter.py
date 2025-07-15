import datetime as dt


class DateQuarter:
    _year: int = 0
    _quarter: int = 0

    def __init__(self, year: int, quarter: int):
        year = year + (quarter - 1) // 4
        quarter = (quarter - 1) % 4 + 1

        self._year = year
        self._quarter = quarter



    @classmethod
    def from_date(cls, date: dt.date):
        return cls(date.year, ((date.month - 1) // 3) + 1)

    def __repr__(self):
        return f"<DateQuarter-Q{self._quarter}-{self._year}>"

    def __str__(self):
        return f"Q{self._quarter}-{self._year}"

    def __contains__(self, item: dt.date):
        return self.__eq__(DateQuarter.from_date(item))

    def __eq__(self, other):
        if isinstance(other, DateQuarter):
            return self._year == other._year and self._quarter == other._quarter
        raise ArithmeticError(f"Cannot determine equlity with type {type(other)}")

    def __gt__(self, other):
        if isinstance(other, dt.date):
            return self.__gt__(DateQuarter.from_date(other))
        if isinstance(other, DateQuarter):
            return self._year > other._year or (self._year == other._year and self._quarter > other._quarter)
        raise ArithmeticError(f"Cannot determine equlity with type {type(other)}")

    def __lt__(self, other):
        if isinstance(other, dt.date):
            return self.__lt__(DateQuarter.from_date(other))
        if isinstance(other, DateQuarter):
            return self._year < other._year or (self._year == other._year and self._quarter < other._quarter)
        raise ArithmeticError(f"Cannot determine equlity with type {type(other)}")

    def __ge__(self, other):
        if isinstance(other, dt.date):
            return self.__ge__(DateQuarter.from_date(other))
        if isinstance(other, DateQuarter):
            return self._year > other._year or (self._year == other._year and self._quarter >= other._quarter)
        raise ArithmeticError(f"Cannot determine equlity with type {type(other)}")

    def __le__(self, other):
        if isinstance(other, dt.date):
            return self.__le__(DateQuarter.from_date(other))
        if isinstance(other, DateQuarter):
            return self._year < other._year or (self._year == other._year and self._quarter <= other._quarter)
        raise ArithmeticError(f"Cannot determine equlity with type {type(other)}")

    def __getitem__(self, item):
        if item == 0:
            return self._year
        elif item == 1:
            return self._quarter
        else:
            raise KeyError()

    def __add__(self, other):
        return DateQuarter(self._year, self._quarter + other)

    def __sub__(self, other):
        if isinstance(other, int):
            return DateQuarter(self._year, self._quarter - other)
        if isinstance(other, DateQuarter):
            quarter = (self._year - other._year) * 4
            quarter += self._quarter - other._quarter
            return quarter
        raise ArithmeticError(f"Cannot subtract type {type(other)}")

    def year(self) -> int:
        return self._year

    def quarter(self) -> int:
        return self._quarter

    def start_date(self) -> dt.date:
        return dt.date(year=self._year, month=(self._quarter - 1) * 3 + 1, day=1)

    def end_date(self) -> dt.date:
        return (self + 1).start_date() - dt.timedelta(days=1)

    def days_in_quarter(self) -> int:
        """
        Return total number of days in quarter
        """
        return ((self + 1).start_date() - self.start_date()).days

    def days_active(self, start_or_end_date: dt.date, is_start_date: bool = False) -> int:
        """
        Return total number of days in quarter before or after a date.\n
        Inclusive of start / end date\n
        When <is_start_date> = True will return the days *after* the provided date
        """
        if is_start_date:
            return (self.end_date() - start_or_end_date).days + 1
        return (start_or_end_date - self.start_date()).days + 1

    def percent_active(self, start_or_end_date: dt.date, is_start_date: bool = False) -> float:
        """
        Return percentage of quarter before or after a date.\n
        Inclusive of start / end date\n
        When <is_start_date> = True will return the days *after* the provided date\n
        See also: active_days_in_quarter()
        """
        return min(round(self.days_active(start_or_end_date, is_start_date) / self.days_in_quarter(), 4), 1.0000)

    def days(self):
        """
        Yield dt.date for each day in quarter
        """
        start = self.start_date()
        end = self.end_date()
        current = start
        while current <= end:
            yield current
            current = current + dt.timedelta(days=1)

    @classmethod
    def between(cls, start: "DateQuarter", end: "DateQuarter", include_last: bool = False):
        """
        Yield DateQuarter for each quarter between start and end quarter.\n
        When <include_last> = True will include end quarter
        """
        delta = 1 if start < end else -1

        current = start
        while current != end:
            yield current
            current += delta

        if include_last:
            yield end