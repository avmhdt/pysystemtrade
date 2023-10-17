"""
Object to store how many days after stop loss
has been activated we can go back to trading
Essentially a counter which goes to 0, with a key that
includes the corresponding Override
"""

from syscore.exceptions import missingData
from syslogging.logger import *
from sysobjects.production.override import Override
from sysobjects.production.delay_days_after_stop_loss import DelayDays
from sysdata.base_data import baseData
from sysobjects.production.tradeable_object import instrumentStrategy
from sysobjects.production.override import override_none

class delayDaysData(baseData):
    def __init__(self, log=get_logger("Delay days")):
        super().__init__(log=log)

    def get_current_delay_for_override(
        self, override: Override
    ) -> DelayDays:

        try:
            current_delay = self._get_current_delay_for_override_no_checking(override)
        except missingData:
            current_delay_object = DelayDays.no_delay()
        else:
            current_delay_object = DelayDays(current_delay)

        return current_delay_object

    def _get_current_delay_for_override_no_checking(
        self, override: Override
    ) -> int:
        raise NotImplementedError("Need to use inheriting class")

    def decrease_current_delay_for_override(
        self, override: Override
    ):
        current_delay_for_override = (
            self.get_current_delay_for_override(
                override
            )
        )

        try:
            current_delay_for_override.decrease()
        except Exception:
            #  FIXME The following is a test, maybe remove later and replace with something else?
            self.delete_delay_for_override(override)


    def delete_delay_for_override(
        self, override: Override
    ):
        raise NotImplementedError("Need to use inheriting class")
