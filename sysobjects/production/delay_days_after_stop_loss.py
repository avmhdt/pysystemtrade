
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
        return "Delay %d days" % self.delay_days

    @property
    def delay_days(self):
        return self._delay_days

    def decrease(self):
        if self.delay_days > 0:
            self._delay_days -= 1
        else:
            raise Exception(
                "Asked to decrease delay days when it was already %d."
                % self.delay_days
            )

    @classmethod
    def no_delay(DelayDays):
        return None
