import datetime
import logging

import numpy as np
from syscore.exceptions import fillExceedsTrade
from sysexecution.orders.named_order_objects import (
    missing_order,
    no_order_id,
    no_children,
    no_parent,
)

from sysexecution.stack_handler.completed_stop_loss_orders import stackHandlerForStopLossCompletions

from sysproduction.data.broker import dataBroker

from sysexecution.orders.contract_orders import contractOrder
from sysexecution.orders.broker_orders import brokerOrder
from sysexecution.trade_qty import tradeQuantity

from sysproduction.data.positions import updatePositions
from sysproduction.data.controls import updateOverrides, updateDelayDays, diagOverrides
from sysproduction.data.orders import dataOrders

from sysobjects.production.override import STOP_LOSS_OVERRIDE
from sysobjects.production.delay_days_after_stop_loss import DelayDays


class stackHandlerForStopLossFills(stackHandlerForStopLossCompletions):
    def process_fills_stop_loss_stack(self):
        """
        Run a regular sweep across the stack
        Doing various things

        :return: success
        """

        self.pass_stop_loss_fills_from_broker_to_broker_stack()
        self.pass_stop_loss_fills_from_broker_up_to_contract()  # this already passes fills to instrument positions

    def pass_stop_loss_fills_from_broker_to_broker_stack(self):
        list_of_broker_order_ids = self.stop_loss_broker_stack.get_list_of_order_ids()
        for broker_order_id in list_of_broker_order_ids:
            self.apply_stop_loss_broker_fill_from_broker_to_broker_database(broker_order_id)

    def apply_stop_loss_broker_fill_from_broker_to_broker_database(self, broker_order_id: int):

        db_broker_order = self.stop_loss_broker_stack.get_order_with_id_from_stack(
            broker_order_id
        )
        if db_broker_order is missing_order:
            return None

        if db_broker_order.fill_equals_desired_trade():
            # No point
            # We don't log or we'd be spamming like crazy
            return None

        data_broker = dataBroker(self.data)
        matched_broker_order = data_broker.match_db_broker_order_to_order_from_brokers(
            db_broker_order
        )

        if matched_broker_order is missing_order:
            log = db_broker_order.log_with_attributes(self.log)
            log.warning(
                "Stop Loss order in database %s does not match any broker orders: can't fill"
                % db_broker_order
            )
            return None

        self.apply_stop_loss_broker_order_fills_to_database(
            broker_order_id=broker_order_id, broker_order=matched_broker_order
        )

    def apply_stop_loss_broker_order_fills_to_database(
        self, broker_order_id: int, broker_order: brokerOrder
    ):

        # Turn commissions into floats
        data_broker = dataBroker(self.data)
        broker_order_with_commissions = (
            data_broker.calculate_total_commission_for_broker_order(broker_order)
        )

        try:
            # This will add commissions, fills, etc
            self.stop_loss_broker_stack.add_execution_details_from_matched_broker_order(
                broker_order_id, broker_order_with_commissions
            )
        except fillExceedsTrade:
            self.log.warning(
                "Fill for exceeds trade for %s, ignoring fill... (hopefully will go away)"
                % str(broker_order)
            )
            return None

        contract_order_id = broker_order.parent

        # pass broker fills upwards
        self.apply_stop_loss_broker_fills_to_contract_order(contract_order_id)

    def pass_stop_loss_fills_from_broker_up_to_contract(self):
        list_of_contract_order_ids = self.stop_loss_contract_stack.get_list_of_order_ids()
        for contract_order_id in list_of_contract_order_ids:
            # this function is in 'core' since it's used elsewhere
            self.apply_stop_loss_broker_fills_to_contract_order(contract_order_id)

    def apply_stop_loss_broker_fills_to_contract_order(self, contract_order_id: int):
        contract_order_before_fill = self.stop_loss_contract_stack.get_order_with_id_from_stack(
            contract_order_id
        )

        children = contract_order_before_fill.children
        if children is no_children:
            # no children created yet, definitely no fills
            return None

        broker_order_list = self.stop_loss_broker_stack.get_list_of_orders_from_order_id_list(
            children
        )

        # We apply: total quantity, average price, highest datetime

        if broker_order_list.all_zero_fills():
            # nothing to do here
            return None

        final_fill_datetime = broker_order_list.final_fill_datetime()
        total_filled_qty = broker_order_list.total_filled_qty()
        average_fill_price = broker_order_list.average_fill_price()

        self.apply_fills_to_stop_loss_contract_order(
            contract_order_before_fill=contract_order_before_fill,
            filled_price=average_fill_price,
            filled_qty=total_filled_qty,
            fill_datetime=final_fill_datetime,
        )

    def apply_stop_loss_contract_order_fill_to_database(self, contract_order: contractOrder):
        contract_order_before_fill = self.stop_loss_contract_stack.get_order_with_id_from_stack(
            contract_order.order_id
        )
        self.apply_fills_to_stop_loss_contract_order(
            contract_order_before_fill=contract_order_before_fill,
            filled_qty=contract_order.fill,
            fill_datetime=contract_order.fill_datetime,
            filled_price=contract_order.filled_price,
        )

    def apply_fills_to_stop_loss_contract_order(
        self,
        contract_order_before_fill: contractOrder,
        filled_qty: tradeQuantity,
        filled_price: float,
        fill_datetime: datetime.datetime,
    ):

        contract_order_id = contract_order_before_fill.order_id
        self.stop_loss_contract_stack.change_fill_quantity_for_order(
            contract_order_id,
            filled_qty,
            filled_price=filled_price,
            fill_datetime=fill_datetime,
        )

        # if fill has changed then update positions
        # we do this here, because we can get here either from fills process
        # or after an execution
        ## At this point the contract stack has changed the contract order to reflect the fill, but the contract_order
        ##    here reflects the original contract order before fills applied, this allows comparision
        self.apply_position_change_to_stored_contract_and_instrument_positions(
            contract_order_before_fill, filled_qty
        )

    def apply_position_change_to_stored_contract_and_instrument_positions(
        self,
        contract_order_before_fill: contractOrder,
        total_filled_qty: tradeQuantity,
        apply_entire_trade: bool = False,
    ):
        current_fills = contract_order_before_fill.fill

        if apply_entire_trade:
            # used for balance trades
            new_fills = current_fills
        else:
            new_fills = total_filled_qty - current_fills

        if new_fills.equals_zero():
            # nothing to do
            return None

        position_updater = updatePositions(self.data)
        position_updater.update_contract_position_table_with_contract_order(
            contract_order_before_fill, new_fills
        )

        ## We now pass it up to the next level
        self.apply_stop_loss_position_change_to_stored_instrument_positions(
            contract_order_before_fill, total_filled_qty
        )

    def apply_stop_loss_position_change_to_stored_instrument_positions(
        self,
        contract_order_before_fill: contractOrder,
        total_filled_qty: tradeQuantity,
        apply_entire_trade: bool = False,
    ):
        current_fills = contract_order_before_fill.fill

        if apply_entire_trade:
            # used for balance trades
            new_fills = current_fills
        else:
            new_fills = total_filled_qty - current_fills

        if new_fills.equals_zero():
            # nothing to do
            return None

        position_updater = updatePositions(self.data)
        position_updater.update_instrument_position_table_with_stop_loss_contract_order(
            contract_order_before_fill, new_fills
        )

        # Set stop loss override and delay days
        self.set_stop_loss_override_for_instrument_strategy_from_contract_order(
            contract_order_before_fill
        )

        ## Order is now potentially completed
        self.handle_completed_stop_loss_contract_order(
            contract_order_before_fill.order_id
        )

    def set_stop_loss_override_for_instrument_strategy_from_contract_order(
        self, contract_order: contractOrder
    ):
        data_orders = dataOrders(self.data)
        parent_instrument_order_id = contract_order.parent
        parent_instrument_order = data_orders.get_historic_instrument_order_from_order_id(
            parent_instrument_order_id
        )

        instrument_strategy = parent_instrument_order.instrument_strategy

        update_overrides = updateOverrides(self.data)
        log = contract_order.log_with_attributes(self.log)

        try:
            update_overrides.update_override_for_instrument_strategy(
                instrument_strategy, STOP_LOSS_OVERRIDE
            )
        except Exception as e:
            msg = "Could not set stop loss override in db for instrument strategy %s! Error msg %s. DB corrupted??" % (
                str(instrument_strategy), str(e)
            )
            log.critical(msg)
            raise Exception(msg)

        self.set_delay_days_for_instrument_strategy_override_from_contract_order(
            contract_order
        )

    def set_delay_days_for_instrument_strategy_override_from_contract_order(
        self, contract_order: contractOrder
    ):
        data_orders = dataOrders(self.data)
        parent_instrument_order_id = contract_order.parent
        parent_instrument_order = data_orders.get_historic_instrument_order_from_order_id(
            parent_instrument_order_id
        )

        instrument_strategy = parent_instrument_order.instrument_strategy
        delay_days_to_set = DelayDays(contract_order.stop_loss_info.delay_days)

        diag_overrides = diagOverrides(self.data)
        log = contract_order.log_with_attributes(self.log)

        update_delay_days = updateDelayDays(self.data)

        override = diag_overrides.get_specific_overrides_for_instrument_strategy_from_db(
            instrument_strategy
        )

        if override is not None:
            try:
                update_delay_days.set_delay_days_for_override(
                    instrument_strategy, delay_days_to_set
                )
            except Exception as e:
                msg = "Could not set delay days in db for instrument strategy %s! Error msg %s. DB corrupted??" % (
                    str(instrument_strategy), str(e)
                )
                log.critical(msg)
                raise Exception(msg)
        else:
            msg = "No override found for %s. Cannot set delay days" % (
                str(instrument_strategy)
            )
            log.critical(msg)
            raise Exception(msg)

