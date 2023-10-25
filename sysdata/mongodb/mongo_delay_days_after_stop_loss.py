from syscore.exceptions import missingData
from sysdata.mongodb.mongo_generic import mongoDataWithMultipleKeys
from sysdata.production.delay_days_after_stop_loss import delayDaysData
from sysobjects.production.delay_days_after_stop_loss import DelayDays
from sysobjects.production.tradeable_object import instrumentStrategy
from syslogging.logger import *

DELAY_DAYS_STATUS_COLLECTION = "current_delay_after_stop_loss"

OVERRIDE_KEY = "override_key"

DELAY_DAYS_KEY = "delay_days"
DELAY_DAYS_VALUE = "value"


class mongoDelayDaysData(delayDaysData):
    """
    Read and write class for delay days data after a stop loss
    is activated for an instrument in a strategy

    """

    def __init__(self, mongo_db=None, log=get_logger("mongoDelayDaysData")):
        super().__init__(log=log)

        self._mongo_data = mongoDataWithMultipleKeys(   # In case more keys are needed later
            DELAY_DAYS_STATUS_COLLECTION, mongo_db=mongo_db
        )

    @property
    def mongo_data(self):
        return self._mongo_data

    def __repr__(self):
        return "Data connection for delay days after stop loss data, mongodb %s"

    def _get_current_delay_for_override_key(
        self, override_key: str
    ) -> DelayDays:

        dict_of_keys = {OVERRIDE_KEY: override_key}
        try:
            result_dict = self.mongo_data.get_result_dict_for_dict_keys(dict_of_keys)
        except missingData:
            return self.default_delay_days()

        delay_days = from_dict_to_delay_days(result_dict)

        return delay_days

    def set_delay_days_for_stop_loss_override(
        self, override_key: str, new_delay: DelayDays
    ):
        dict_of_keys = {OVERRIDE_KEY: override_key}
        delay_as_dict = from_delay_days_to_dict(new_delay)

        self.mongo_data.add_data(dict_of_keys, delay_as_dict, allow_overwrite=True)

    def delete_delay_days_for_stop_loss_override(
        self, override_key: str
    ):
        dict_of_keys = {OVERRIDE_KEY: override_key}
        self.mongo_data.delete_data_without_any_warning(dict_of_keys)


def from_dict_to_delay_days(result_dict: dict) -> DelayDays:
    value = result_dict[DELAY_DAYS_VALUE]
    delay_days = DelayDays(value)
    return delay_days


def from_delay_days_to_dict(delay_days: DelayDays) -> dict:
    delay_days_as_value = delay_days.as_numeric_value()
    delay_days_as_dict = {DELAY_DAYS_VALUE: delay_days_as_value}

    return delay_days_as_dict
