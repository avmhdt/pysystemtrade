
DEFAULT_DELAY_DAYS = int(0)


class DelayDays(object):

    def __init__(self, value: int):
        if value < 0:
            raise Exception(
                "Delay days must be positive integer, value received is %d"
                % value
            )

        self._delay_days = value

    def __repr__(self):
        return "Delay %s days" % str(self.delay_days)

    @property
    def delay_days(self):
        return self._delay_days

    def decrease(self):
        if self.delay_days > 0:
            self._delay_days -= 1
        else:
            raise Exception(
                "Asked to decrease delay days when it was already %s."
                % str(self.delay_days)
            )

    @classmethod
    def no_delay(DelayDays):
        return DelayDays(DEFAULT_DELAY_DAYS)

    def as_numeric_value(self):
        return int(self.delay_days)

    def is_zero(self):
        return self.delay_days == 0
