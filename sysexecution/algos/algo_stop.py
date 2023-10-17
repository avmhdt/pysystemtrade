"""
Stop order execution method for stop loss
"""
from copy import copy
from sysexecution.orders.named_order_objects import missing_order

from sysexecution.algos.algo import Algo
from sysexecution.algos.common_functions import (
    post_trade_processing,
    MESSAGING_FREQUENCY,
    cancel_order,
    file_log_report_market_order,
)
from sysexecution.order_stacks.broker_order_stack import orderWithControls
from sysexecution.orders.broker_orders import stop_order_type, brokerOrderType

class algoStop(Algo):
    """
    Stop execution algo for attaching stop loss to
    existing positions.
    Submits a single stop order for entire amount
    or modifies existing stop order, changing its amount to match new entries
    Algo is not blocking as it sends the order and shuts down
    therefore it does not require order management
    """

    def submit_trade(self) -> orderWithControls:
        broker_order_with_controls = self.prepare_and_submit_trade()
        if broker_order_with_controls is missing_order:
            # something went wrong
            return missing_order

        return broker_order_with_controls

    def prepare_and_submit_trade(self):
        contract_order = self.contract_order
        log = contract_order.log_with_attributes(self.data.log)

        order_type = self.order_type_to_use
        broker_order_with_controls = (
            self.get_and_submit_broker_order_for_contract_order(
                contract_order, order_type=order_type
            )
        )

        return broker_order_with_controls

    @property
    def order_type_to_use(self) -> brokerOrderType:
        return stop_order_type