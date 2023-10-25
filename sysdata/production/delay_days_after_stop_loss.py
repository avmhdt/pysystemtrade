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

    @property
    def default_delay_days(self):
        return DelayDays.no_delay()

    def get_delay_days_for_stop_loss_override(
        self, override_key: str
    ) -> DelayDays:

        try:
            current_delay = self._get_current_delay_for_override_key(override_key)
        except missingData:
            current_delay = DelayDays.no_delay()

        return current_delay

    def _get_current_delay_for_override_key(
        self, override_key: str
    ) -> int:
        raise NotImplementedError("Need to use inheriting class")

    def decrease_current_delay_for_override(
        self, override_key: str
    ):
        current_delay_for_override = (
            self.get_delay_days_for_stop_loss_override(
                override_key
            )
        )

        try:
            current_delay_for_override.decrease()
        except Exception as already_zero:
            self.log.debug(already_zero)
        else:
            self.set_delay_days_for_stop_loss_override(
                override_key, current_delay_for_override
            )

    def set_delay_days_for_stop_loss_override(
        self, override_key: str, new_delay: DelayDays
    ):
        raise NotImplementedError("Need to use inheriting class")

    def delete_delay_days_for_stop_loss_override(
        self, override_key: str
    ):
        raise NotImplementedError("Need to use inheriting class")

