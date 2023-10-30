from sysexecution.stack_handler.spawn_children_from_instrument_orders import (
    stackHandlerForSpawning,
)
from sysexecution.stack_handler.roll_orders import stackHandlerForRolls
from sysexecution.stack_handler.create_broker_orders_from_contract_orders import (
    stackHandlerCreateBrokerOrders,
)
from sysexecution.stack_handler.cancel_and_modify import stackHandlerCancelAndModify
from sysexecution.stack_handler.checks import stackHandlerChecks
from sysexecution.stack_handler.additional_sampling import (
    stackHandlerAdditionalSampling,
)
from sysexecution.stack_handler.create_stop_loss_broker_orders_from_fills import (
    stackHandlerCreateStopLossBrokerOrders
)


class stackHandler(
    stackHandlerForSpawning,
    stackHandlerForRolls,
    stackHandlerCreateBrokerOrders,
    stackHandlerCancelAndModify,
    stackHandlerChecks,
    stackHandlerAdditionalSampling,
    stackHandlerCreateStopLossBrokerOrders,
):
    def safe_stack_removal(self):
        # Safe deletion of stack
        # We do this at the end of every day as we don't like state hanging
        # around

        self.log.debug("Running safe stack removal")
        # First, cancel any partially or unfilled broker orders
        self.log.debug("Trying to cancel all broker orders")
        self.cancel_and_confirm_all_broker_orders(log_critical_on_timeout=True)

        # Next, process fills
        self.log.debug("Processing fills")
        self.process_fills_stack()

        # and then completions
        # need special flag for completions, since we also need to 'complete' partially filled orders
        # and allow empty broker orders to be marked as completed
        self.log.debug("Processing completions")
        self.handle_completed_orders(
            allow_partial_completions=True, allow_zero_completions=True
        )

        # Process stop loss fills
        self.log.debug("Processing stop loss fills")
        self.process_fills_stop_loss_stack()

        # and then completions
        self.log.debug("Processing stop loss completions")
        self.handle_completed_stop_loss_orders(
            allow_partial_completions=True, allow_zero_completions=True
        )

        self.remove_all_deactivated_orders_from_stack()

    def remove_all_deactivated_orders_from_stack(self):
        # Now we can delete everything
        self.instrument_stack.remove_all_deactivated_orders_from_stack()
        self.contract_stack.remove_all_deactivated_orders_from_stack()
        self.broker_stack.remove_all_deactivated_orders_from_stack()
        self.stop_loss_contract_stack.remove_all_deactivated_orders_from_stack()
        self.stop_loss_broker_stack.remove_all_deactivated_orders_from_stack()
