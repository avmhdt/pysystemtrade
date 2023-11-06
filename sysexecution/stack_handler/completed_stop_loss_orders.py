from sysexecution.orders.named_order_objects import missing_order, no_children

from sysexecution.stack_handler.stackHandlerCore import stackHandlerCore, orderFamily
from sysproduction.data.orders import dataOrders


class stackHandlerForStopLossCompletions(stackHandlerCore):
    def handle_completed_stop_loss_orders(
        self,
        allow_partial_completions: bool = False,
        allow_zero_completions: bool = False,
        treat_inactive_as_complete: bool = False,
    ):
        list_of_completed_stop_loss_contract_orders = (
            self.stop_loss_contract_stack.list_of_completed_order_ids(
                allow_partial_completions=allow_partial_completions,
                allow_zero_completions=allow_zero_completions,
                treat_inactive_as_complete=treat_inactive_as_complete,
            )
        )

        for stop_loss_contract_order_id in list_of_completed_stop_loss_contract_orders:
            self.handle_completed_stop_loss_contract_order(
                stop_loss_contract_order_id,
                allow_partial_completions=allow_partial_completions,
                allow_zero_completions=allow_zero_completions,
            )

    def handle_completed_stop_loss_contract_order(
        self,
        stop_loss_contract_order_id: int,
        allow_partial_completions=False,
        allow_zero_completions=False,
        treat_inactive_as_complete=False,
    ):
        # Check children all done

        completely_filled = self.confirm_all_children_are_filled(
            stop_loss_contract_order_id,
            allow_partial_completions=allow_partial_completions,
            allow_zero_completions=allow_zero_completions,
            treat_inactive_as_complete=treat_inactive_as_complete,
        )

        if not completely_filled:
            return None

        # If we have got this far then all our children are filled, and the
        # parent is filled

        # the 'order family' will now include split orders
        order_family = self.get_order_family_for_stop_loss_contract_order_id(
            stop_loss_contract_order_id
        )

        # Make orders inactive
        # A subsequent process will delete them
        self.deactivate_family_of_stop_loss_orders(order_family)

        self.add_order_family_to_historic_orders_database(order_family)

    def get_order_family_for_stop_loss_contract_order_id(
        self, stop_loss_contract_order_id: int
    ) -> orderFamily:

        stop_loss_contract_order = self.stop_loss_contract_stack.get_order_with_id_from_stack(
            stop_loss_contract_order_id
        )

        list_of_broker_order_id = (
            self.get_all_children_from_list_of_contract_order_id(
                stop_loss_contract_order_id
            )
        )

        order_family = orderFamily(
            instrument_order_id=None,
            list_of_contract_order_id=[stop_loss_contract_order_id],
            list_of_broker_order_id=list_of_broker_order_id,
        )

        return order_family

    def get_all_children_from_list_of_contract_order_id(
        self, stop_loss_contract_order_id: int
    ) -> list:
        list_of_broker_order_id = []

        contract_order = self.stop_loss_contract_stack.get_order_with_id_from_stack(
            stop_loss_contract_order_id
        )

        broker_order_children = contract_order.children
        if broker_order_children is not no_children:
            list_of_broker_order_id = (
                list_of_broker_order_id + broker_order_children
            )

        list_of_broker_order_id = list(set(list_of_broker_order_id))

        return list_of_broker_order_id

    def confirm_all_children_are_filled(
        self,
        stop_loss_contract_order_id: int,
        allow_partial_completions=False,
        allow_zero_completions=False,
        treat_inactive_as_complete=True,
    ):

        order_family = self.get_order_family_for_stop_loss_contract_order_id(
            stop_loss_contract_order_id
        )

        order_filled = self.stop_loss_contract_stack.is_completed(
            stop_loss_contract_order_id,
            allow_zero_completions=allow_zero_completions,
            allow_partial_completions=allow_partial_completions,
            treat_inactive_as_complete=treat_inactive_as_complete,
        )

        # Should be only one child order:
        children_filled = self.check_list_of_stop_loss_broker_orders_complete(
            order_family.list_of_broker_order_id,
            allow_partial_completions=allow_partial_completions,
            allow_zero_completions=allow_zero_completions,
            treat_inactive_as_complete=treat_inactive_as_complete,
        )

        if order_filled and children_filled:
            return True
        else:
            return False

    def check_list_of_stop_loss_broker_orders_complete(
        self,
        list_of_broker_order_id: list,
        allow_partial_completions=False,
        allow_zero_completions=False,
        treat_inactive_as_complete=False,
    ):

        for broker_order_id in list_of_broker_order_id:
            completely_filled = self.stop_loss_broker_stack.is_completed(
                broker_order_id,
                allow_zero_completions=allow_zero_completions,
                allow_partial_completions=allow_partial_completions,
                treat_inactive_as_complete=treat_inactive_as_complete,
            )
            if not completely_filled:
                # OK We can't do this unless all our children are filled
                return False

        return True

    def add_order_family_to_historic_orders_database(self, order_family: orderFamily):

        instrument_order = self.instrument_stack.get_order_with_id_from_stack(
            order_family.instrument_order_id
        )
        contract_order_list = self.stop_loss_contract_stack.get_list_of_orders_from_order_id_list(
            order_family.list_of_contract_order_id
        )
        broker_order_list = self.stop_loss_broker_stack.get_list_of_orders_from_order_id_list(
            order_family.list_of_broker_order_id
        )

        # Update historic order database
        order_data = dataOrders(self.data)
        order_data.add_historic_orders_to_data(     # FIXME create add_historic_stop_loss_orders_to_data in orders.py
            instrument_order, contract_order_list, broker_order_list
        )

    def deactivate_family_of_stop_loss_orders(self, order_family: orderFamily):

        # Make orders inactive
        # A subsequent process will delete them

        for contract_order_id in order_family.list_of_contract_order_id:
            self.stop_loss_contract_stack.deactivate_order(contract_order_id)

        for broker_order_id in order_family.list_of_broker_order_id:
            self.stop_loss_broker_stack.deactivate_order(broker_order_id)
