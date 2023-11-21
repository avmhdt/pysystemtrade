import datetime
from collections import namedtuple
from copy import copy
from typing import List

import numpy as np
import pandas as pd

from syscore.genutils import transfer_object_attributes
from syscore.pandas.pdutils import make_df_from_list_of_named_tuple
from sysobjects.production.delay_days_after_stop_loss import DelayDays
from sysproduction.data.broker import dataBroker
from sysproduction.data.instruments import diagInstruments
from sysproduction.data.orders import dataOrders
from sysproduction.data.positions import diagPositions
from sysproduction.data.controls import diagOverrides, diagDelayDays
from sysproduction.reporting.data.risk import (
    get_current_annualised_stdev_for_instrument,
)
from sysexecution.orders.broker_orders import (
    brokerOrderWithParentInformation,
    stopLossBrokerOrderWithInstrumentAndParentInfo,
)
from sysdata.data_blob import dataBlob
from sysobjects.production.override import STOP_LOSS_OVERRIDE, Override

from dataclasses import dataclass

def get_current_stop_loss_broker_orders(data: dataBlob):
    data_orders = dataOrders(data)
    order_id_list = data_orders.get_current_stop_loss_broker_order_ids()
    orders_as_list = [
        get_tuple_object_for_current_orders_from_order_id(data, order_id) for order_id in order_id_list
    ]
    pdf = make_df_from_list_of_named_tuple(currentOrdersData, orders_as_list)

    return pdf


def get_tuple_object_for_current_orders_from_order_id(data, order_id):
    data_orders = dataOrders(data)
    diag_positions = diagPositions(data)
    order = data_orders.get_current_stop_loss_broker_order_from_order_id_with_parent_data(
        order_id
    )
    position = diag_positions.get_current_position_for_instrument_strategy(
        order.instrument_strategy
    )
    order_with_parent_position = (
        stopLossBrokerOrderWithInstrumentAndParentInfo.add_parent_position_to_order_object(
            order, position
        )
    )

    tuple_object = transfer_object_attributes(
        currentOrdersData, order_with_parent_position
    )

    return tuple_object


currentOrdersData = namedtuple(
    "ordersData",
    [
        "order_id",
        "instrument_code",
        "strategy_name",
        "contract_date",
        "instrument_strategy_position",
        "grandparent_filled_price",
        "grandparent_fill_datetime",
        "stop_price",
        "stop_loss_percent_difference",
        "trade",
        "buy_or_sell",
    ],
)


def get_recent_filled_stop_loss_broker_order_ids(
    data: dataBlob, start_date, end_date
):
    data_orders = dataOrders(data)
    order_id_list = data_orders.get_historic_stop_loss_broker_order_ids_in_date_range(
        period_start=start_date, period_end=end_date
    )
    orders_as_list = [
        get_tuple_object_for_historic_orders_from_order_id(data, order_id) for order_id in order_id_list
    ]
    pdf = make_df_from_list_of_named_tuple(tradesData, orders_as_list)

    return pdf


def get_tuple_object_for_historic_orders_from_order_id(data, order_id):
    data_orders = dataOrders(data)
    order = data_orders.get_historic_stop_loss_broker_order_from_order_id_with_execution_data(
        order_id
    )
    tuple_object = transfer_object_attributes(tradesData, order)

    return tuple_object


tradesData = namedtuple(
    "tradesData",
    [
        "order_id",
        "instrument_code",
        "strategy_name",
        "contract_date",
        "fill",
        "filled_price",
        "mid_price",
        "side_price",
        "offside_price",
        "parent_reference_price",  # from contract order
        "parent_reference_datetime",  # from instrument order
        "submit_datetime",
        "fill_datetime",
        "limit_price",
        "trade",
        "buy_or_sell",
        "parent_limit_price",
        "commission",
        "stop_price",
    ],
)


def get_stop_loss_overrides_and_delay_days_as_df(data: dataBlob):
    diag_overrides = diagOverrides(data)
    diag_delay_days = diagDelayDays(data)

    all_overrides = diag_overrides.get_dict_of_all_overrides_in_db_with_reasons()
    stop_loss_overrides = get_stop_loss_overrides_from_dict_of_all_overrides(all_overrides)

    corresponding_delay_days = (
        [
            diag_delay_days.get_delay_days_for_stop_loss_override(
                override_key=key
            )
            for key in stop_loss_overrides.keys()
        ]
    )

    instrument_codes = [key.instrument_code for key in stop_loss_overrides.keys()]
    strategy_names = [key.strategy_name for key in stop_loss_overrides.keys()]
    overrides = list(stop_loss_overrides.values())
    delay_days = [value.delay_days for value in corresponding_delay_days]

    overrides_and_delays = OverridesAndDelays(
        instrument_code=instrument_codes,
        strategy_name=strategy_names,
        override=overrides,
        delay_days=delay_days,
    )

    tuple_object = transfer_object_attributes(overrideDelayDaysData, overrides_and_delays)

    return tuple_object


def get_stop_loss_overrides_from_dict_of_all_overrides(
    all_overrides: dict
) -> dict:
    stop_loss_overrides = dict(
        [
            (
                key, override
            )
            for key, override in all_overrides.items() if override is STOP_LOSS_OVERRIDE
        ]
    )

    return stop_loss_overrides


overrideDelayDaysData = namedtuple(
    "overrideDelayDaysData",
    [
        'instrument_code',
        'strategy_name',
        'override',
        'delay_days',
    ],
)

@dataclass
class OverridesAndDelays:
    instrument_code: List[str]
    strategy_name: List[str]
    override: List[Override]
    delay_days: List[DelayDays]


