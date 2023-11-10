"""
Stack handler is a giant object, so we split it up into files/classes

This 'core' is inherited by all the other classes and just initialises, plus does some common functions

"""
from collections import namedtuple
from syscore.constants import arg_not_supplied, success, failure
from syscore.exceptions import missingData

from sysdata.data_blob import dataBlob

from sysexecution.order_stacks.order_stack import orderStackData, failureWithRollback
from sysexecution.orders.base_orders import Order
from sysexecution.orders.list_of_orders import listOfOrders

from sysproduction.data.orders import dataOrders
from sysproduction.data.prices import diagPrices, updatePrices
from sysproduction.data.contracts import dataContracts
from sysproduction.data.broker import dataBroker
from sysproduction.data.positions import updatePositions

from sysexecution.order_stacks.stop_loss_contract_order_stack import stopLossContractOrderStackData
from sysexecution.orders.base_orders import stopLossInfo
from sysexecution.orders.contract_orders import contractOrder, contractOrderType
from sysobjects.contracts import futuresContract


class stackHandlerCore(object):
    def __init__(self, data: dataBlob = arg_not_supplied):
        if data is arg_not_supplied:
            data = dataBlob()

        self._data = data
        self._log = data.log

        order_data = dataOrders(data)

        instrument_stack = order_data.db_instrument_stack_data
        contract_stack = order_data.db_contract_stack_data
        broker_stack = order_data.db_broker_stack_data

        stop_loss_contract_stack = order_data.db_stop_loss_contract_stack_data
        stop_loss_broker_stack = order_data.db_stop_loss_broker_stack_data

        self._instrument_stack = instrument_stack
        self._contract_stack = contract_stack
        self._broker_stack = broker_stack

        self._stop_loss_contract_stack = stop_loss_contract_stack
        self._stop_loss_broker_stack = stop_loss_broker_stack

    @property
    def data(self):
        return self._data

    @property
    def log(self):
        return self._log

    @property
    def instrument_stack(self):
        return self._instrument_stack

    @property
    def contract_stack(self):
        return self._contract_stack

    @property
    def broker_stack(self):
        return self._broker_stack

    @property
    def stop_loss_contract_stack(self):
        return self._stop_loss_contract_stack

    @property
    def stop_loss_broker_stack(self):
        return self._stop_loss_broker_stack

    @property
    def diag_prices(self) -> diagPrices:
        diag_prices = getattr(self, "_diag_prices", None)
        if diag_prices is None:
            diag_prices = diagPrices(self.data)
            self._diag_prices = diag_prices

        return diag_prices

    @property
    def data_contracts(self) -> dataContracts:
        data_contracts = getattr(self, "_data_contracts", None)
        if data_contracts is None:
            data_contracts = dataContracts(self.data)
            self._data_contracts = data_contracts

        return data_contracts

    @property
    def data_broker(self) -> dataBroker:
        data_broker = getattr(self, "_data_broker", None)
        if data_broker is None:
            data_broker = dataBroker(self.data)
            self._data_broker = data_broker

        return data_broker

    @property
    def update_prices(self) -> updatePrices:
        update_prices = getattr(self, "_update_prices", None)
        if update_prices is None:
            update_prices = updatePrices(self.data)
            self._update_prices = update_prices

        return update_prices


def put_children_on_stack(
    child_stack: orderStackData,
    parent_order: Order,
    list_of_child_orders: listOfOrders,
    parent_log,
) -> list:

    try:
        list_of_child_ids = child_stack.put_list_of_orders_on_stack(
            list_of_child_orders
        )
    except failureWithRollback as e:
        parent_log.warning(
            "Tried to add child orders but %s; rolled back so can try again (parent %s)"
            % (str(e), str(parent_order))
        )
        return []

    except Exception as e:
        parent_log.critical(
            "Tried to add child orders, error %s and couldn't roll back! Order stack may well be corrupted!"
            % str(e)
        )
        return []

    return list_of_child_ids


def add_children_to_parent_or_rollback_children(
    parent_order: Order,
    list_of_child_ids: list,
    parent_stack: orderStackData,
    child_stack: orderStackData,
    parent_log,
):
    try:
        parent_stack.add_children_to_order_without_existing_children(
            parent_order.order_id, list_of_child_ids
        )
    except Exception as e:
        try:
            child_stack.rollback_list_of_orders_on_stack(list_of_child_ids)
            parent_log.warning(
                "Tried to add child orders to parent but %s; rolled back so can try again (parent %s)"
                % (str(e), str(parent_order))
            )
            return failure
        except:
            parent_log.critical(
                "Tried to add child orders, error %s and couldn't roll back! Order stack may well be corrupted!"
                % str(e)
            )
            return failure

    return success


def log_successful_adding(
    list_of_child_orders: listOfOrders,
    list_of_child_ids: list,
    parent_order: Order,
    parent_log,
):

    for child_order, child_id in zip(list_of_child_orders, list_of_child_ids):
        child_log = child_order.log_with_attributes(parent_log)
        child_log.debug(
            "Put child order %s on stack with ID %d from parent order %s"
            % (str(child_order), child_id, str(parent_order))
        )


def rollback_parents_and_children_and_handle_exceptions(
    parent_stack: orderStackData,
    child_stack: orderStackData,
    parent_order_id: int,
    list_of_child_order_ids: list,
    parent_log,
    error_from_adding_child_orders: Exception,
):

    ##
    try:
        rollback_parents_and_children(
            child_stack=child_stack,
            parent_stack=parent_stack,
            list_of_child_order_ids=list_of_child_order_ids,
            parent_order_id=parent_order_id,
        )
        parent_log.warning(
            "Error %s when adding a set of parents and children but managed to rollback"
            % str(error_from_adding_child_orders)
        )
        return None

    except Exception as rollback_exception:
        ## bloody hell even the rollback has failed, throw everything out of the pram
        parent_log.critical(
            "Error %s when adding a set of parents and children and couldn't rollback got error %s! Stack may be corrupted"
            % (str(error_from_adding_child_orders), str(rollback_exception))
        )
        return None


def rollback_parents_and_children(
    parent_stack: orderStackData,
    child_stack: orderStackData,
    parent_order_id: int,
    list_of_child_order_ids: list,
):

    ## parent order might be locked
    parent_stack.unlock_order_on_stack(parent_order_id)
    parent_stack.deactivate_order(parent_order_id)
    parent_stack.remove_order_with_id_from_stack(parent_order_id)

    # If any children, roll them back also
    if len(list_of_child_order_ids) > 0:
        child_stack.rollback_list_of_orders_on_stack(list_of_child_order_ids)


#   FIXME Change this and add stop loss contract and broker order ids:
orderFamily = namedtuple(
    "orderFamily",
    ["instrument_order_id", "list_of_contract_order_id", "list_of_broker_order_id"],
)


def add_stop_loss_level_and_delay_from_config_to_stop_loss_info(
    data: dataBlob, stop_loss_info: stopLossInfo, log
) -> stopLossInfo:
    stop_loss_config = getattr(data.config, 'stop_loss', None)
    if stop_loss_config is None:
        log.critical(
            "Missing stop loss information from private_config.yaml!!!"
        )
        raise Exception('stop_loss missing from private_config.yaml')

    try:
        if stop_loss_config['use_catastrophic']:
            if stop_loss_info.stop_loss_level is arg_not_supplied:
                stop_loss_info.stop_loss_level = stop_loss_config['catastrophic_level']
            if stop_loss_info.delay_days is arg_not_supplied:
                stop_loss_info.delay_days = stop_loss_config['delay_days_after_stop_loss']
    except KeyError:
        log.critical('Missing use_catastrophic parameter from stop loss config! Considering it to be False!')

    return stop_loss_info


def find_stop_loss_contract_order_for_contract_in_stack(
    contract: futuresContract, stop_loss_contract_stack: stopLossContractOrderStackData
) -> contractOrder:
    instrument_code = contract.instrument.instrument_code

    list_of_existing_stop_loss_orders = (
        stop_loss_contract_stack.get_list_of_orders(
            exclude_inactive_orders=True
        )
    )

    list_of_stop_loss_orders_for_requested_instrument = []
    for order in list_of_existing_stop_loss_orders:
        if order.tradeable_object.instrument_code == instrument_code:
            list_of_stop_loss_orders_for_requested_instrument.append(order)

    if len(list_of_stop_loss_orders_for_requested_instrument) > 1:
        raise Exception(
            "More than 1 existing stop loss order at broker for instrument %s!"
            % str(instrument_code)
        )
    elif len(list_of_stop_loss_orders_for_requested_instrument) == 0:
        raise missingData(
            "No stop loss orders for requested instrument %s" % (
                str(instrument_code)
            )
        )

    return list_of_stop_loss_orders_for_requested_instrument[0]


def get_stop_loss_level_percent_difference_from_current_price(
    data: dataBlob, existing_stop_loss_contract_order: contractOrder
) -> float:
    diag_prices = diagPrices(data)

    futures_contract = (
        existing_stop_loss_contract_order.futures_contract
    )

    current_contract_price = (
        diag_prices.get_prices_at_frequency_for_contract_object(
            futures_contract, frequency='D'
        )
    )

    stop_loss_price = existing_stop_loss_contract_order.stop_price

    percent_diff_between_stop_loss_order_and_contract_price = (
        abs((stop_loss_price / current_contract_price) - 1.0)
    )

    return percent_diff_between_stop_loss_order_and_contract_price

