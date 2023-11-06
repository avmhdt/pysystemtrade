from copy import copy
from numpy import sign
from itertools import compress
from syscore.objects import (
    resolve_function,
)
from syscore.constants import arg_not_supplied
from syscore.exceptions import missingData

from sysobjects.contracts import futuresContract

from sysexecution.orders.named_order_objects import missing_order
from sysexecution.trade_qty import tradeQuantity

from sysexecution.orders.named_order_objects import no_order_id, no_children, no_parent
from sysexecution.orders.contract_orders import contractOrder, contractOrderType
from sysexecution.orders.broker_orders import brokerOrder
from sysexecution.order_stacks.broker_order_stack import orderWithControls
from sysexecution.stack_handler.roll_orders import ROLL_PSEUDO_STRATEGY
from sysexecution.stack_handler.stop_loss_fills import stackHandlerForStopLossFills
from sysexecution.algos.algo import Algo

from sysproduction.data.controls import (
    dataLocks,
    dataTradeLimits,
)
from sysproduction.data.positions import diagPositions
from sysexecution.orders.base_orders import (
    NEW_ORDER,
    CHANGE_EXISTING_ORDER,
    NO_STOP_LOSS,
    stopLossInfo,
)

from sysdata.data_blob import dataBlob


class stackHandlerCreateStopLossBrokerOrders(stackHandlerForStopLossFills):
    def create_stop_loss_broker_orders_from_fills(self):
        """
        Create stop loss orders from corresponding parent contract order fills.

        As soon as a regular broker order is filled, the stack handler passes fills upwards
        to the broker, contract, and instrument stacks. This class checks for these fills
        at the contract level, and sends the corresponding stop loss broker order at the price
        that has been specified by config files, as well as by the strategy order generator and
        stackHandlerForSpawning.

        """
        filled_contract_orders = self.check_for_fills_in_contract_stack()
        for filled_contract_order in filled_contract_orders:
            stop_loss_order = (
                self.create_stop_loss_broker_order_from_fill_if_necessary(
                    filled_contract_order
                )
            )
            """
            # Code below already does this:
            if stop_loss_order is not missing_order and stop_loss_order is not None:
                self.store_stop_loss_broker_order_in_db_stack(stop_loss_order)
            
            We need to store stop_loss_contract_stack and stop_loss_broker_stack in db at end of day 
            (stack handler shutdown) and load this info at beginning of trading (stack handler start)
            """

    def check_for_fills_in_contract_stack(self):

        contract_stack = self.contract_stack
        """
        contract_stack_order_list = contract_stack.get_list_of_orders(
            exclude_inactive_orders=False
        )

        filled_contract_order_list = []

        for contract_order in contract_stack_order_list:
            if contract_order.filled_qty == contract_order.trade:
                filled_contract_order_list.append(contract_order)

        """
        filled_contract_order_list = contract_stack.list_of_completed_order_ids(
            allow_partial_completions=False,
            allow_zero_completions=False,
            treat_inactive_as_complete=True
        )

        return filled_contract_order_list

    def create_stop_loss_broker_order_from_fill_if_necessary(
        self, filled_contract_order: contractOrder
    ) -> orderWithControls:

        strategy = filled_contract_order.strategy_name

        if strategy == ROLL_PSEUDO_STRATEGY:
            return self.create_stop_loss_broker_orders_from_filled_roll_order(
                filled_contract_order
            )

        return self.create_stop_loss_broker_order_from_filled_strategy_order(
            filled_contract_order
        )

    def create_stop_loss_broker_orders_from_filled_roll_order(
        self, filled_contract_order: contractOrder
    ) -> orderWithControls:

        attach_stop_loss = (
            filled_contract_order.stop_loss_info.attach_stop_loss
        )

        if attach_stop_loss == NEW_ORDER:
            # Spread order (FORCE roll):
            if len(filled_contract_order.trade) > 1:
                return self.create_stop_loss_broker_order_for_spread_force_roll(
                    filled_contract_order
                )
            # Outright orders (FORCE OUTRIGHT)
            else:
                return self.create_stop_loss_broker_order_for_outright_roll(
                    filled_contract_order
                )

        # Close first contract or outright roll priced (old) contract
        elif attach_stop_loss == CHANGE_EXISTING_ORDER:
            return self.create_stop_loss_broker_order_close_first_contract(
                filled_contract_order
            )

        elif attach_stop_loss == NO_STOP_LOSS:
            # Do nothing, no stop loss
            return None

        else:
            raise Exception(
                "Invalid value for attach_stop_loss, order id %s"
                % str(filled_contract_order.order_id)
            )

    def create_stop_loss_broker_order_for_spread_force_roll(
        self, filled_contract_order: contractOrder
    ) -> orderWithControls:

        log = filled_contract_order.log_with_attributes(self.log)

        # Get existing stop loss for priced contract:
        try:
            order_to_change_priced_contract = (
                self.get_existing_stop_loss_order_for_futures_contract(
                    filled_contract_order.tradeable_object[0].futures_contract,     # [0] for priced contract
                    strategy_name=filled_contract_order.strategy_name,
                )
            )
        except Exception:
            msg = "More than 1 existing stop loss orders for contract %s!" % (
                filled_contract_order.tradeable_object[0].futures_contract
            )
            log.critical(msg)
            raise Exception(msg)

        # Cancel order:
        if order_to_change_priced_contract is not missing_order:
            self.change_existing_stop_loss_order_size(
                order_to_change_priced_contract,
                -order_to_change_priced_contract.fill.as_single_trade_qty_or_error()
            )

        # Get (maybe) existing stop loss order for forward contract
        # If order_to_change_priced_contract is not missing_order, this should be missing_order
        try:
            order_to_change_forward_contract = (
                self.get_existing_stop_loss_order_for_futures_contract(
                    filled_contract_order.tradeable_object[1].futures_contract,  # [1] for forward contract
                    strategy_name=filled_contract_order.strategy_name,
                )
            )
        except Exception:
            msg = "More than 1 existing stop loss orders for contract %s!" % (
                filled_contract_order.tradeable_object[1].futures_contract
            )
            log.critical(msg)
            raise Exception(msg)

        # If there is no stop loss for forward contract already:
        if order_to_change_forward_contract is missing_order:
            filled_price_forward_contract = get_filled_prices_for_forward_contract_from_spread_fill(
                self.data, filled_contract_order
            )
            filled_order_for_forward_leg = contractOrder(
                filled_contract_order.strategy_name,
                filled_contract_order.instrument_code,
                filled_contract_order.contract_date_key,
                filled_contract_order.trade[1],
                fill=filled_contract_order.fill,
                filled_price=filled_price_forward_contract,
                order_id=filled_contract_order.order_id,
                parent=filled_contract_order.parent,
                children=filled_contract_order.children,
                order_type=filled_contract_order.order_type,
                reference_price=filled_contract_order.reference_price,
                roll_order=filled_contract_order.roll_order,
                stop_loss_info=filled_contract_order.stop_loss_info,
            )

            return self.create_and_send_new_stop_loss_broker_order(
                filled_order_for_forward_leg
            )
        # If there is a stop loss order for forward contract already do nothing
        else:
            return None

    def create_stop_loss_broker_order_for_outright_roll(
        self, filled_contract_order: contractOrder
    ) -> orderWithControls:

        stop_loss_order_already_exists_and_is_correct_size = (  # FIXME remove the 'is_correct_size'?
            self.check_for_existing_stop_loss_order_in_stack(
                filled_contract_order
            )
        )

        if not stop_loss_order_already_exists_and_is_correct_size:
            return self.create_and_send_new_stop_loss_broker_order(
                filled_contract_order
            )

        return None

    def create_stop_loss_broker_order_close_first_contract(
        self, filled_contract_order: contractOrder
    ) -> orderWithControls:

        log = filled_contract_order.log_with_attributes(self.log)

        try:
            order_to_change = (
                self.get_existing_stop_loss_order_for_futures_contract(
                    filled_contract_order.futures_contract,
                    strategy_name=filled_contract_order.strategy_name,
                )
            )
        except Exception:
            msg = "More than 1 existing stop loss orders for contract %s!" % (
                filled_contract_order.futures_contract
            )
            log.critical(msg)
            raise Exception(msg)

        change_order_by = filled_contract_order.stop_loss_info.change_order_by

        self.change_existing_stop_loss_order_size(
            order_to_change, change_order_by
        )

        return None

    def create_stop_loss_broker_order_from_filled_strategy_order(
        self, filled_contract_order: contractOrder
    ) -> orderWithControls:

        log = filled_contract_order.log_with_attributes(self.log)

        stop_loss_order_already_exists_and_is_correct_size = (
            self.check_for_existing_stop_loss_order_in_stack(
                filled_contract_order
            )
        )

        if not stop_loss_order_already_exists_and_is_correct_size:

            attach_stop_loss = (
                filled_contract_order.stop_loss_info.attach_stop_loss
            )

            if attach_stop_loss == NEW_ORDER:
                try:
                    order_to_change = (
                        self.get_existing_stop_loss_order_for_futures_contract(
                            filled_contract_order.futures_contract,
                            strategy_name=filled_contract_order.strategy_name,
                        )
                    )
                except Exception:
                    msg = "More than 1 existing stop loss orders for contract %s!" % (
                        filled_contract_order.futures_contract
                    )
                    log.critical(msg)
                    raise Exception(msg)

                if order_to_change is not missing_order:
                    # Direction change. Cancel old stop loss order:
                    self.change_existing_stop_loss_order_size(
                        order_to_change, -order_to_change.fill.as_single_trade_qty_or_error()
                    )

                return self.create_and_send_new_stop_loss_broker_order(
                    filled_contract_order
                )

            elif attach_stop_loss == CHANGE_EXISTING_ORDER:
                try:
                    order_to_change = (
                        self.get_existing_stop_loss_order_for_futures_contract(
                            filled_contract_order.futures_contract,
                            strategy_name=filled_contract_order.strategy_name,
                        )
                    )
                except Exception:
                    msg = "More than 1 existing stop loss orders for contract %s!" % (
                        filled_contract_order.futures_contract
                    )
                    log.critical(msg)
                    raise Exception(msg)

                change_order_by = filled_contract_order.stop_loss_info.change_order_by

                self.change_existing_stop_loss_order_size(
                    order_to_change, change_order_by
                )

                return None

            elif attach_stop_loss == NO_STOP_LOSS:
                return None

            else:
                raise Exception(
                    "Invalid value for attach_stop_loss, order id %s"
                    % str(filled_contract_order.order_id)
                )

        return None

    def check_for_existing_stop_loss_order_in_stack(
        self, filled_contract_order: contractOrder
    ) -> bool:

        diag_positions = diagPositions(self.data)
        stop_loss_contract_stack = self.stop_loss_contract_stack  # stop_loss_broker_stack
        
        list_of_orders_in_stack = stop_loss_contract_stack.get_list_of_orders(
            exclude_inactive_orders=True
        )
        
        relevant_contract = filled_contract_order.futures_contract
        instrument_strategy = filled_contract_order.instrument_strategy

        contract_position_at_db = (
            diag_positions.get_position_for_contract(relevant_contract)
        )

        for order in list_of_orders_in_stack:
            if order.futures_contract == relevant_contract and (
                order.instrument_strategy == instrument_strategy
            ) and (
                order.trade.as_single_trade_qty_or_error() == contract_position_at_db
            ):
                return True

        return False

    def create_and_send_new_stop_loss_broker_order(
        self, filled_contract_order: contractOrder
    ) -> orderWithControls:

        stop_loss_contract_order = (
            self.create_and_put_stop_loss_contract_order_on_stack(
                filled_contract_order
            )
        )

        stop_loss_contract_order = (
            self.preprocess_stop_loss_contract_order(stop_loss_contract_order)
        )

        if stop_loss_contract_order is missing_order:
            # Empty order not submitting to algo
            return None

        algo_instance_and_placed_broker_order_with_controls = self.send_to_stop_loss_algo(
            stop_loss_contract_order
        )

        if algo_instance_and_placed_broker_order_with_controls is missing_order:
            # something gone wrong with execution
            return missing_order

        (
            algo_instance,
            placed_broker_order_with_controls,
        ) = algo_instance_and_placed_broker_order_with_controls

        broker_order_with_controls_and_order_id = self.add_stop_loss_trade_to_database(
            placed_broker_order_with_controls
        )

        return broker_order_with_controls_and_order_id

    def create_and_put_stop_loss_contract_order_on_stack(
        self, filled_contract_order: contractOrder
    ):
        log = filled_contract_order.log_with_attributes(self.log)

        stop_loss_info = filled_contract_order.stop_loss_info
        stop_loss_info = self.fill_remaining_stop_loss_info(stop_loss_info, log)

        trade, stop_loss_price, delay_days = (
            resolve_inputs_to_stop_loss_order(
                log, stop_loss_info, filled_contract_order
            )
        )

        stop_loss_contract_order = contractOrder(
            filled_contract_order.strategy_name,
            filled_contract_order.instrument_code,
            filled_contract_order.contract_date_key,
            trade,
            parent=filled_contract_order.parent,
            order_type=contractOrderType("stop"),
            stop_price=stop_loss_price,
            stop_loss_info=stop_loss_info,
        )

        self.stop_loss_contract_stack.put_order_on_stack(stop_loss_contract_order)

        return stop_loss_contract_order

    def fill_remaining_stop_loss_info(
        self, stop_loss_info: stopLossInfo, log
    ) -> stopLossInfo:

        if stop_loss_info.attach_stop_loss == NO_STOP_LOSS:
            return stop_loss_info

        stop_loss_config = self.data.config.get_element("stop_loss")

        if stop_loss_info.stop_loss_level is arg_not_supplied:
            try:
                stop_loss_info.stop_loss_level = (
                    stop_loss_config['catastrophic_level']
                )

            except KeyError:
                msg = "catastrophic_level missing from config!!!"
                log.critical(msg)
                raise missingData(msg)

        if stop_loss_info.delay_days is arg_not_supplied:
            try:
                stop_loss_info.delay_days = (
                    stop_loss_config['delay_days_after_stop_loss']
                )

            except KeyError:
                msg = "delay_days_after_stop_loss missing from config!!!"
                log.critical(msg)
                raise missingData(msg)

        return stop_loss_info

    def preprocess_stop_loss_contract_order(
        self, stop_loss_contract_order: contractOrder
    ) -> contractOrder:

        if stop_loss_contract_order is missing_order:
            # weird race condition
            return missing_order

        if stop_loss_contract_order.is_order_controlled_by_algo():
            # already being traded by an active algo
            return missing_order

        data_broker = self.data_broker

        # CHECK FOR LOCKS
        data_locks = dataLocks(self.data)
        instrument_locked = data_locks.is_instrument_locked(
            stop_loss_contract_order.instrument_code
        )

        market_closed = not (
            data_broker.is_contract_okay_to_trade(
                stop_loss_contract_order.futures_contract
            )
        )
        if instrument_locked or market_closed:
            # we don't log to avoid spamming
            # print("market is closed for order %s" % str(original_contract_order))
            return missing_order

        # RESIZE
        stop_loss_contract_order_to_trade = (
            self.size_stop_loss_contract_order(stop_loss_contract_order)
        )

        return stop_loss_contract_order_to_trade

    def size_stop_loss_contract_order(
        self, original_contract_order: contractOrder
    ) -> contractOrder:

        # Check the order doesn't breach trade limits
        contract_order_to_trade = self.apply_trade_limits_to_contract_order(
            original_contract_order
        )

        # Don't resize for liquidity as this is a catastrophic stop loss order

        if contract_order_to_trade is missing_order:
            return missing_order

        if contract_order_to_trade.fill_equals_desired_trade():
            # Nothing left to trade
            return missing_order

        return contract_order_to_trade

    def apply_trade_limits_to_contract_order(
        self, proposed_order: contractOrder
    ) -> contractOrder:
        log = proposed_order.log_with_attributes(self.log)
        data_trade_limits = dataTradeLimits(self.data)

        instrument_strategy = proposed_order.instrument_strategy

        # proposed_order.trade.total_abs_qty() is a scalar, returns a scalar
        maximum_abs_qty = (
            data_trade_limits.what_trade_is_possible_for_strategy_instrument(
                instrument_strategy, proposed_order.trade
            )
        )

        contract_order_after_trade_limits = (
            proposed_order.change_trade_size_proportionally_to_meet_abs_qty_limit(
                maximum_abs_qty
            )
        )

        if contract_order_after_trade_limits.trade != proposed_order.trade:
            log.debug(
                "%s trade change from %s to %s because of trade limits"
                % (
                    proposed_order.key,
                    str(proposed_order.trade),
                    str(contract_order_after_trade_limits.trade),
                )
            )

        return contract_order_after_trade_limits

    def send_to_stop_loss_algo(
        self, contract_order_to_trade: contractOrder
    ) -> (Algo, orderWithControls):

        log = contract_order_to_trade.log_with_attributes(self.log)
        config = self.data.config.get_element("execution_algos")

        contract_order_to_trade_with_algo_set = copy(contract_order_to_trade)
        try:
            stop_loss_algo = config["stop_loss_algo"]
        except KeyError:
            error_msg = 'Missing stop_loss_algo from config files!!!'
            log.critical(error_msg)
            raise Exception(error_msg)

        contract_order_to_trade_with_algo_set.algo_to_use = stop_loss_algo

        log.debug(
            "Sending order %s to algo %s"
            % (
                str(contract_order_to_trade_with_algo_set),
                contract_order_to_trade_with_algo_set.algo_to_use,
            )
        )

        algo_class_to_call = self.add_controlling_algo_to_order(
            contract_order_to_trade_with_algo_set
        )
        algo_instance = algo_class_to_call(
            self.data, contract_order_to_trade_with_algo_set
        )

        # THIS LINE ACTUALLY SENDS THE ORDER TO THE ALGO
        placed_broker_order_with_controls = algo_instance.submit_trade()

        if placed_broker_order_with_controls is missing_order:
            # important we do this or order will never execute
            #  if no issue here will be released once order filled
            self.stop_loss_contract_stack.release_order_from_algo_control(
                contract_order_to_trade_with_algo_set.order_id
            )
            return missing_order

        return algo_instance, placed_broker_order_with_controls

    def add_controlling_algo_to_order(
        self, contract_order_to_trade: contractOrder
    ) -> "function":
        # Note we don't save the algo method, but reallocate each time
        # This is useful if trading is about to finish, because we switch to market orders
        # (assuming a bunch of limit orders haven't worked out so well)

        algo_to_use_str = contract_order_to_trade.algo_to_use
        algo_method = resolve_function(algo_to_use_str)

        # This prevents another algo from trying to trade the same contract order
        # Very important to avoid multiple broker orders being issued from the
        # same contract order
        self.stop_loss_contract_stack.add_controlling_algo_ref(
            contract_order_to_trade.order_id, algo_to_use_str
        )

        return algo_method

    def add_stop_loss_trade_to_database(
        self, broker_order_with_controls: orderWithControls
    ) -> orderWithControls:
        broker_order_with_controls_and_order_id = copy(broker_order_with_controls)

        stop_loss_broker_order = broker_order_with_controls_and_order_id.order

        log = stop_loss_broker_order.log_with_attributes(self.log)
        try:
            stop_loss_broker_order_id = self.stop_loss_broker_stack.put_order_on_stack(stop_loss_broker_order)
        except Exception as e:
            # We've created a broker order but can't add it to the broker order database
            # Probably safest to leave the contract order locked otherwise there could be multiple
            #   broker orders issued and nobody wants that!
            error_msg = (
                "Created a stop loss broker order %s but can't add it to the order stack!! (condition %s) STACK CORRUPTED"
                % (str(stop_loss_broker_order), str(e))
            )
            log.critical(error_msg)
            raise Exception(error_msg)

        # set order_id (wouldn't have had one before, might be done inside db adding but make explicit)
        stop_loss_broker_order.order_id = stop_loss_broker_order_id

        # This broker order is a child of the parent contract order
        # We add 'another' child since it's valid to have multiple broker
        # orders
        stop_loss_contract_order_id = stop_loss_broker_order.parent
        self.stop_loss_contract_stack.add_another_child_to_order(
            stop_loss_contract_order_id, stop_loss_broker_order_id
        )

        return broker_order_with_controls_and_order_id

    def get_existing_stop_loss_order_for_futures_contract(
        self, futures_contract: futuresContract, strategy_name: str
    ) -> contractOrder:

        list_of_existing_stop_loss_orders = (
            self.stop_loss_contract_stack.get_list_of_orders(
                exclude_inactive_orders=True
            )
        )

        list_of_stop_loss_orders_for_contract = []

        for existing_stop_loss_order in list_of_existing_stop_loss_orders:
            if existing_stop_loss_order.futures_contract == (
                futures_contract
            ) and existing_stop_loss_order.strategy_name == strategy_name:
                list_of_stop_loss_orders_for_contract.append(existing_stop_loss_order)

        number_of_existing_orders_for_contract = len(list_of_stop_loss_orders_for_contract)

        if number_of_existing_orders_for_contract > 1:
            raise Exception(
                "%s existing stop loss orders for %s contract!!!"
                % (
                    str(number_of_existing_orders_for_contract),
                    str(futures_contract)
                )
            )

        elif number_of_existing_orders_for_contract == 0:
            return missing_order

        return list_of_stop_loss_orders_for_contract[0]

    def change_existing_stop_loss_order_size(
        self, order_to_change: contractOrder, change_order_by: int
    ):

        log = order_to_change.log_with_attributes(self.log)
        stop_loss_contract_stack = self.stop_loss_contract_stack
        filled_qty = order_to_change.fill.as_single_trade_qty_or_error()

        if change_order_by == -filled_qty:
            stop_loss_contract_stack.deactivate_order(order_to_change.order_id)
            return None

        definitely_change_order_by = self.apply_trade_limits_to_contract_order_size_change(
            order_to_change, change_order_by
        )

        try:
            stop_loss_contract_stack.change_existing_order_trade_qty(
                order_to_change.order_id, filled_qty + definitely_change_order_by
            )
        except Exception as e:
            error_msg = (
                "Tried to change stop loss contract order trade quantity for %s but couldn't!! (condition %s) MAYBE STACK IS CORRUPTED"
                % (str(order_to_change), str(e))
            )
            log.critical(error_msg)
            raise Exception(error_msg)

        self.check_for_stop_loss_contract_order_changes_and_propagate_to_broker()

    def apply_trade_limits_to_contract_order_size_change(
        self, order_to_change: contractOrder, change_order_by: int
    ) -> int:
        log = order_to_change.log_with_attributes(self.log)
        data_trade_limits = dataTradeLimits(self.data)
        abs_change_order_by = abs(change_order_by)
        instrument_strategy = order_to_change.instrument_strategy

        maximum_abs_qty = data_trade_limits.what_trade_is_possible_for_strategy_instrument(
            instrument_strategy, tradeQuantity(abs_change_order_by)
        )

        if maximum_abs_qty != abs_change_order_by:
            log.debug(
                "%s trade change from %s to %s because of trade limits"
                % (
                    order_to_change.key,
                    str(change_order_by),
                    str(maximum_abs_qty),
                )
            )

        if sign(change_order_by) == -1:
            return -maximum_abs_qty

        return maximum_abs_qty

    def check_for_stop_loss_contract_order_changes_and_propagate_to_broker(self):

        stop_loss_contract_stack = self.stop_loss_contract_stack

        list_of_contract_orders = stop_loss_contract_stack.get_list_of_orders(
            exclude_inactive_orders=True
        )

        for contract_order in list_of_contract_orders:
            log = contract_order.log_with_attributes(self.log)

            children_broker_orders = contract_order.children
            if children_broker_orders is no_children:
                msg = "Stop loss contract order %s has no children" % (
                    str(contract_order)
                )
                log.debug(msg)
                pass

            if len(children_broker_orders) > 1:
                msg = "Stop loss contract order %s has more than one child at broker!" % (
                    str(contract_order)
                )
                log.critical(msg)
                raise Exception(msg)

            else:
                child_broker_order = children_broker_orders[0]
                if child_broker_order.trade != contract_order.trade:
                    self.propagate_stop_loss_contract_order_change_to_broker_level(
                        contract_order, child_broker_order
                    )

    def propagate_stop_loss_contract_order_change_to_broker_level(
        self, parent_contract_order: contractOrder, child_broker_order: orderWithControls
    ):
        actual_child_broker_order = child_broker_order.order
        log = actual_child_broker_order.log_with_attributes(self.log)
        stop_loss_broker_stack = self.stop_loss_broker_stack

        try:
            stop_loss_broker_stack.change_existing_order_trade_qty(
                actual_child_broker_order.order_id, parent_contract_order.trade
            )
        except Exception as e:
            error_msg = (
                    "Tried to propagate stop loss contract order %s trade quantity change for to broker order %s but couldn't!! (condition %s) MAYBE STACK IS CORRUPTED"
                    % (str(parent_contract_order), str(actual_child_broker_order), str(e))
            )
            log.critical(error_msg)
            raise Exception(error_msg)


def resolve_inputs_to_stop_loss_order(
    log, stop_loss_info: stopLossInfo, filled_contract_order: contractOrder,
) -> (int, float, int):

    trade = [
        stop_loss_info.change_order_by,
        -filled_contract_order.fill.as_single_trade_qty_or_error(),
    ]

    # Check to see if change_order_by has same sign as filled order
    if trade[0] != 0 and sign(trade[0]) != sign(trade[1]):
        msg = "sign(stop_loss_info.change_order_by) != sign(filled_contract_order.fill.as_single_trade_qty_or_error())"
        log.critical(msg)
        raise Exception(msg)

    trade = list(
        compress(
            trade,
            [
                abs(x) == min([abs(y) for y in trade])
                for x in trade
            ]
        )
    )[0]

    if stop_loss_info.attach_stop_loss == NEW_ORDER:
        if trade < 0:
            stop_loss_price = (
                filled_contract_order.filled_price * (
                    1. - stop_loss_info.stop_loss_level
                )
            )

        elif trade > 0:
            stop_loss_price = (
                filled_contract_order.filled_price * (
                    1. + stop_loss_info.stop_loss_level
                )
            )

        else:
            stop_loss_price = arg_not_supplied

    else:
        stop_loss_price = arg_not_supplied

    delay_days = stop_loss_info.delay_days

    return (
        trade, stop_loss_price, delay_days
    )


def get_filled_prices_for_forward_contract_from_spread_fill(
    data: dataBlob, filled_spread_order: contractOrder
) -> float:

    log = filled_spread_order.log_with_attributes(data.log)

    broker_children = filled_spread_order.children

    if broker_children is no_children:
        msg = "Filled contract spread order %s has no children. How can this be??" % (
            str(filled_spread_order)
        )
        log.critical(msg)
        raise Exception(msg)

    filled_prices = [broker_child.leg_filled_prices[1] for broker_child in broker_children]
    forward_trade = filled_spread_order.trade[1]

    if forward_trade > 0:
        filled_price = min(filled_prices)
    elif forward_trade < 0:
        filled_price = max(filled_prices)
    else:
        msg = "Filled contract spread order %s has no forward trade, or equals zero. How can this be??" % (
            str(filled_spread_order)
        )
        log.critical(msg)
        raise Exception(msg)

    return filled_price

