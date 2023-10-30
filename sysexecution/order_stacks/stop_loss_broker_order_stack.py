import datetime
import logging
from copy import copy

from sysexecution.orders.named_order_objects import missing_order
from sysexecution.order_stacks.order_stack import missingOrder
from sysexecution.order_stacks.broker_order_stack import brokerOrderStackData
from sysexecution.trade_qty import tradeQuantity

from sysexecution.orders.broker_orders import brokerOrder


class stopLossBrokerOrderStackData(brokerOrderStackData):
    def _name(self):
        return "Stop loss broker order stack"

    def change_existing_order_trade_qty(self, order_id: int, new_quantity: int):

        old_order = self.get_order_with_id_from_stack(order_id)
        log = old_order.log_with_attributes(self.log)

        new_order = brokerOrder(
            old_order.strategy_name,
            old_order.instrument_code,
            old_order.contract_date_key,
            new_quantity,
            locked=old_order.is_order_locked(),
            parent=old_order.parent,
            children=old_order.children,
            active=old_order.active,
            order_type=old_order.order_type,
            algo_used=old_order.algo_used,
            algo_comment=old_order.algo_comment,
            limit_price=old_order.limit_price,
            submit_datetime=old_order.submit_datetime,
            side_price=old_order.side_price,
            mid_price=old_order.mid_price,
            offside_price=old_order.offside_price,
            broker=old_order.broker,
            broker_account=old_order.broker_account,
            broker_clientid=old_order.broker_clientid,
            broker_permid=old_order.broker_permid,
            broker_tempid=old_order.broker_tempid,
            manual_fill=old_order.manual_fill,
            stop_price=old_order.stop_price,
        )

        try:
            self._change_order_on_stack(order_id, new_order)
        except Exception as e:
            msg = "Could not change order size for %s! (Exception %s)" % (
                str(old_order), str(e)
            )
            log.critical(msg)
            raise Exception(msg)

