import datetime
import logging
from copy import copy

from sysexecution.orders.named_order_objects import missing_order
from sysexecution.order_stacks.order_stack import missingOrder
from sysexecution.order_stacks.contract_order_stack import contractOrderStackData
from sysexecution.trade_qty import tradeQuantity

from sysexecution.orders.contract_orders import contractOrder


class stopLossContractOrderStackData(contractOrderStackData):
    def _name(self):
        return "Stop loss contract order stack"

    def change_existing_order_trade_qty(self, order_id: int, new_quantity: int):

        old_order = self.get_order_with_id_from_stack(order_id)
        log = old_order.log_with_attributes(self.log)

        new_order = contractOrder(
            old_order.strategy_name,
            old_order.instrument_code,
            old_order.contract_date_key,
            new_quantity,
            order_type=old_order.order_type,
            stop_price=old_order.stop_price,
            stop_loss_info=old_order.stop_loss_info,
        )

        try:
            self._change_order_on_stack(order_id, new_order)
        except Exception as e:
            msg = "Could not change order size for %s! (Exception %s)" % (
                str(old_order), str(e)
            )
            log.critical(msg)
            raise Exception(msg)

