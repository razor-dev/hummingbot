import asyncio
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.coincheck import (
    coincheck_constants as CONSTANTS,
    coincheck_utils,
    coincheck_web_utils as web_utils,
)
from hummingbot.connector.exchange.coincheck.coincheck_api_order_book_data_source import CoincheckAPIOrderBookDataSource
from hummingbot.connector.exchange.coincheck.coincheck_api_user_stream_data_source import (
    CoincheckAPIUserStreamDataSource,
)
from hummingbot.connector.exchange.coincheck.coincheck_auth import CoincheckAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import DeductedFromReturnsTradeFee, TokenAmount, TradeFeeBase, TradeFeeSchema
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

class CoincheckExchange(ExchangePyBase):
    UPDATE_ORDER_STATUS_MIN_INTERVAL = 10.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 coincheck_api_key: str,
                 coincheck_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = coincheck_api_key
        self.secret_key = coincheck_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        self._last_trades_poll_coincheck_timestamp = 1.0
        super().__init__(client_config_map)

    @staticmethod
    def coincheck_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(coincheck_type: str) -> OrderType:
        return OrderType[coincheck_type]

    @property
    def authenticator(self):
        return CoincheckAuth(
            access_key=self.api_key,
            secret_key=self.secret_key)

    @property
    def name(self) -> str:
        return "coincheck"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.LIMIT_MAKER, OrderType.MARKET]

    async def get_all_pairs_prices(self) -> List[Dict[str, str]]:
        pairs_prices = await self._api_get(path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL)
        return pairs_prices

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        is_time_synchronizer_related = ("-1021" in error_description
                                        and "Timestamp for this request" in error_description)
        return is_time_synchronizer_related

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return str(CONSTANTS.ORDER_NOT_EXIST_ERROR_CODE) in str(
            status_update_exception
        ) and CONSTANTS.ORDER_NOT_EXIST_MESSAGE in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return str(CONSTANTS.UNKNOWN_ORDER_ERROR_CODE) in str(
            cancelation_exception
        ) and CONSTANTS.UNKNOWN_ORDER_MESSAGE in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            domain=self._domain,
            auth=self._auth)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return CoincheckAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return CoincheckAPIUserStreamDataSource(
            auth=self._auth,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> TradeFeeBase:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return DeductedFromReturnsTradeFee(percent=self.estimate_fee_pct(is_maker))

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        order_result = None
        amount_str = f"{amount:f}"
        type_str = CoincheckExchange.coincheck_order_type(order_type)
        side_str = CONSTANTS.SIDE_BUY if trade_type is TradeType.BUY else CONSTANTS.SIDE_SELL
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        api_params = {"symbol": symbol,
                      "side": side_str,
                      "quantity": amount_str,
                      "type": type_str,
                      "newClientOrderId": order_id}
        if order_type is OrderType.LIMIT or order_type is OrderType.LIMIT_MAKER:
            price_str = f"{price:f}"
            api_params["price"] = price_str
        if order_type == OrderType.LIMIT:
            api_params["timeInForce"] = CONSTANTS.TIME_IN_FORCE_GTC

        try:
            order_result = await self._api_post(
                path_url=CONSTANTS.ORDER_PATH_URL,
                data=api_params,
                is_auth_required=True)
            o_id = str(order_result["id"])
            transact_time = order_result["created_at"]
        except IOError as e:
            error_description = str(e)
            is_server_overloaded = ("status is 503" in error_description
                                    and "Unknown error, please check your request or try again later." in error_description)
            if is_server_overloaded:
                o_id = "UNKNOWN"
                transact_time = self._time_synchronizer.time()
            else:
                raise
        return o_id, transact_time

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        api_params = {
            "symbol": symbol,
            "id": order_id,
        }
        cancel_result = await self._api_delete(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params=api_params,
            is_auth_required=True)
        if cancel_result.get("status") == "CANCELED":
            return True
        return False

    async def _format_trading_rules(self, exchange_info_dict: Dict[str, Any]) -> List[TradingRule]:
        """
        Formats the trading rules retrieved from the exchange to TradingRule objects used by Hummingbot.
        :param exchange_info_dict: the raw response from the exchange's API
        :return: A list of TradingRule objects
        """
        trading_pair_rules = exchange_info_dict.get("exchange_status", [])
        retval = []

        for rule in filter(coincheck_utils.is_exchange_information_valid, trading_pair_rules):
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("pair"))
                filters = rule.get("filters")

                # Safely access filters to prevent index errors
                price_filter = next((f for f in filters if f.get("filterType") == "PRICE_FILTER"), None)
                lot_size_filter = next((f for f in filters if f.get("filterType") == "LOT_SIZE"), None)
                min_notional_filter = next(
                    (f for f in filters if f.get("filterType") in ["MIN_NOTIONAL", "NOTIONAL"]),
                    None)

                if price_filter and lot_size_filter and min_notional_filter:
                    # Safely extract and convert values
                    min_order_size = Decimal(lot_size_filter.get("minQty"))
                    tick_size = Decimal(price_filter.get("tickSize"))
                    step_size = Decimal(lot_size_filter.get("stepSize"))
                    min_notional = Decimal(min_notional_filter.get("minNotional"))

                    retval.append(
                        TradingRule(
                            trading_pair=trading_pair,
                            min_order_size=min_order_size,
                            min_price_increment=tick_size,
                            min_base_amount_increment=step_size,
                            min_notional_size=min_notional
                        )
                    )

            except Exception as e:
                self.logger().exception(f"Error parsing the trading pair rule {rule.get('pair')}: {e}")

        return retval

    async def _status_polling_loop_fetch_updates(self):
        await self._update_order_fills_from_trades()
        await super()._status_polling_loop_fetch_updates()

    async def _update_trading_fees(self):
        """
        Fetches the trading fees for each pair from the Coincheck account API and stores them.
        """
        # account_info = await self._api_request(path=CONSTANTS.ACCOUNTS_PATH_URL, method=RESTMethod.GET)
        #
        # # Check if the response is successful
        # if account_info["success"]:
        #     exchange_fees = account_info.get("exchange_fees", {})
        #
        #     # Clear any previously stored fees
        #     self._trading_fees.clear()
        #
        #     # Loop through each pair and extract the fees
        #     for pair, fee_info in exchange_fees.items():
        #         taker_fee = Decimal(fee_info["taker_fee"]) / Decimal("100")  # Convert to decimal percentage
        #         maker_fee = Decimal(fee_info["maker_fee"]) / Decimal("100")  # Convert to decimal percentage
        #
        #         # Store the fees in a dictionary
        #         self._trading_fees[pair] = TradeFeeSchema(
        #             maker_percent_fee_decimal=maker_fee,
        #             taker_percent_fee_decimal=taker_fee
        #         )
        # else:
        #     self.logger().error(f"Failed to fetch trading fees: {account_info}")
        pass

    async def _user_stream_event_listener(self):
        """
        This function runs in the background continuously processing the events received from the exchange by the user
        stream data source. It keeps reading events from the queue until the task is interrupted.
        The events received are balance updates, order updates, and trade events.
        """
        async for event_message in self._iter_user_event_queue():
            try:
                event_type = event_message.get("type")
                if event_type == "executionReport":
                    client_order_id = event_message.get("id")
                    if event_message.get("status") == "filled":
                        tracked_order = self._order_tracker.all_fillable_orders.get(client_order_id)
                        if tracked_order is not None:
                            fee = TradeFeeBase.new_spot_fee(
                                fee_schema=self.trade_fee_schema(),
                                trade_type=tracked_order.trade_type,
                                percent_token=event_message["commissionAsset"],
                                flat_fees=[TokenAmount(amount=Decimal(event_message["commission"]),
                                                       token=event_message["commissionAsset"])])
                            trade_update = TradeUpdate(
                                trade_id=str(event_message["id"]),
                                client_order_id=client_order_id,
                                exchange_order_id=str(event_message["orderId"]),
                                trading_pair=tracked_order.trading_pair,
                                fee=fee,
                                fill_base_amount=Decimal(event_message["qty"]),
                                fill_quote_amount=Decimal(event_message["qty"]) * Decimal(event_message["price"]),
                                fill_price=Decimal(event_message["price"]),
                                fill_timestamp=event_message["time"] * 1e-3,
                            )
                            self._order_tracker.process_trade_update(trade_update)

                    tracked_order = self._order_tracker.all_updatable_orders.get(client_order_id)
                    if tracked_order is not None:
                        order_update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=event_message["created_at"] * 1e-3,
                            new_state=CONSTANTS.ORDER_STATE[event_message["status"]],
                            client_order_id=client_order_id,
                            exchange_order_id=str(event_message["orderId"]),
                        )
                        self._order_tracker.process_order_update(order_update=order_update)

                elif event_type == "account_update":
                    balances = event_message["balances"]
                    for balance_entry in balances:
                        asset_name = balance_entry["asset"]
                        free_balance = Decimal(balance_entry["free"])
                        total_balance = Decimal(balance_entry["free"]) + Decimal(balance_entry["locked"])
                        self._account_available_balances[asset_name] = free_balance
                        self._account_balances[asset_name] = total_balance

            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().error(f"Unexpected error in user stream listener loop: {e}", exc_info=True)
                await self._sleep(5.0)

    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []
        if order.exchange_order_id is not None:
            exchange_order_id = int(order.exchange_order_id)
            trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
            all_fills_response = await self._api_get(
                path_url=CONSTANTS.MY_TRADES_PATH_URL,
                params={"pair": trading_pair, "orderId": exchange_order_id},
                is_auth_required=True
            )

            for trade in all_fills_response["transactions"]:
                fee = TradeFeeBase.new_spot_fee(
                    fee_schema=self.trade_fee_schema(),
                    trade_type=order.trade_type,
                    percent_token=trade["commissionAsset"],
                    flat_fees=[TokenAmount(amount=Decimal(trade["commission"]),
                                           token=trade["commissionAsset"])]
                )
                trade_update = TradeUpdate(
                    trade_id=str(trade["id"]),
                    client_order_id=order.client_order_id,
                    exchange_order_id=str(trade["orderId"]),
                    trading_pair=trading_pair,
                    fee=fee,
                    fill_base_amount=Decimal(trade["funds"]["jpy"]),
                    fill_quote_amount=Decimal(trade["funds"]["jpy"]),
                    fill_price=Decimal(trade["funds"]["jpy"]),
                    fill_timestamp=trade["created_at"] * 1e-3,
                )
                trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> OrderUpdate:
        trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        updated_order_data = await self._api_get(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params={"pair": trading_pair, "orderId": tracked_order.exchange_order_id},
            is_auth_required=True
        )

        new_state = CONSTANTS.ORDER_STATE[updated_order_data["status"]]

        order_update = OrderUpdate(
            client_order_id=tracked_order.client_order_id,
            exchange_order_id=str(updated_order_data["id"]),
            trading_pair=tracked_order.trading_pair,
            update_timestamp=updated_order_data["created_at"] * 1e-3,
            new_state=new_state,
        )

        return order_update

    async def _update_balances(self):
        """
        --fixed--
        Fetches the balance information from Coincheck and updates available and total balances.
        """
        balance_info = await self._api_request(path_url=CONSTANTS.ACCOUNTS_BALANCE_PATH_URL, method=RESTMethod.GET,
                                               is_auth_required=True)

        # Clear previous balances
        self._account_available_balances.clear()
        self._account_balances.clear()

        # Loop through each asset in the response
        for asset, available_balance in balance_info.items():
            # Skip any non-asset entries like "success"
            if asset.endswith("_reserved") or asset.endswith("_lend_in_use") or asset.endswith(
                    "_debt") or asset.endswith("_lent") or asset.endswith("_tsumitate"):
                continue

            # Reserved balance (if present)
            reserved_balance = Decimal(balance_info.get(f"{asset}_reserved", "0.0"))

            # Available balance
            available_balance = Decimal(available_balance)

            # Total balance is the sum of available and reserved
            total_balance = available_balance + reserved_balance

            # Update available balances and total balances
            self._account_available_balances[asset] = available_balance
            self._account_balances[asset] = total_balance

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: Dict[str, Any]):
        """
        --fixed--
        Initializes the trading pair symbol map from the Coincheck exchange info.
        Filters only the trading pairs that are available and can place orders.
        :param exchange_info: A dictionary containing the exchange information.
        """
        mapping = bidict()

        for symbol_data in exchange_info["exchange_status"]:
            pair = symbol_data["pair"]  # Example: 'btc_jpy'
            status = symbol_data["status"]
            availability = symbol_data["availability"]



            # Only add pairs that are available and allow order placement
            if status == "available" and availability["order"]:
                # Split the pair into base and quote (assumes pair format is 'base_quote')
                base, quote = pair.split("_")

                # Format the pair into Hummingbot’s expected format (e.g., 'BTC-JPY')
                hb_trading_pair = combine_to_hb_trading_pair(base=base.upper(), quote=quote.upper())

                # Map the exchange's pair format to Hummingbot's format
                mapping[pair] = hb_trading_pair
        # Set the trading pair symbol map in the connector
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "pair": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params=params
        )

        return float(resp_json["last"])  # Assuming "last" contains the last traded price in Coincheck's response
