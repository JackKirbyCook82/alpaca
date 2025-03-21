# -*- coding: utf-8 -*-
"""
Created on Tues Feb 18 2025
@name:   Aplaca Order Objects
@author: Jack Kirby Cook

"""

import types
import numpy as np
import pandas as pd

from finance.variables import Querys, Variables, Securities, OSI
from webscraping.weburl import WebURL, WebPayload
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from support.mixins import Emptying, Logging, Naming

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


instrument_parser = lambda string: Variables.Securities.Instrument.OPTION if any(list(map(str.isdigit, string))) else Variables.Securities.Instrument.STOCK
ticker_parser = lambda string: OSI(string).ticker if instrument_parser(string) == Variables.Securities.Instrument.OPTION else str(string).upper()
expire_parser = lambda string: OSI(string).expire if instrument_parser(string) == Variables.Securities.Instrument.OPTION else None
option_parser = lambda string: OSI(string).option if instrument_parser(string) == Variables.Securities.Instrument.OPTION else Variables.Securities.Option.EMPTY
strike_parser = lambda string: OSI(string).strike if instrument_parser(string) == Variables.Securities.Instrument.OPTION else None
limit_parser = lambda string: np.round(float(string), 2) if not isinstance(string, types.NoneType) else None
stop_parser = lambda string: np.round(float(string), 2) if not isinstance(string, types.NoneType) else None
position_parser = lambda string: {"buy": Variables.Securities.Position.LONG, "sell": Variables.Securities.Position.SHORT}[string]
term_parser = lambda string: {"market": Variables.Markets.Term.MARKET, "limit": Variables.Markets.Term.LIMIT}[string]
tenure_parser = lambda string: {"day": Variables.Markets.Tenure.DAY, "fok": Variables.Markets.Tenure.FILLKILL}[string]
action_parser = lambda string: {"buy": Variables.Markets.Action.BUY, "sell": Variables.Markets.Action.SELL}[string]

stock_formatter = lambda security: str(security.ticker).upper()
option_formatter = lambda security: str(OSI([security.ticker, security.expire, security.option, security.strike]))
security_formatter = lambda security: {Variables.Securities.Instrument.STOCK: stock_formatter, Variables.Securities.Instrument.OPTION: option_formatter}[security.instrument](security)
quantity_formatter = lambda security: {Variables.Securities.Instrument.STOCK: "100", Variables.Securities.Instrument.OPTION: "1"}[security.instrument]
action_formatter = lambda security: {Variables.Securities.Position.LONG: "buy", Variables.Securities.Position.SHORT: "sell"}[security.position]
tenure_formatter = lambda order: {Variables.Markets.Tenure.DAY: "day", Variables.Markets.Tenure.FILLKILL: "fok"}[order.tenure]
term_formatter = lambda order: {Variables.Markets.Term.MARKET: "market", Variables.Markets.Term.LIMIT: "limit"}[order.term]


class AlpacaSecurity(Naming, fields=["ticker", "expire", "instrument", "option", "position", "strike"]):
    def __str__(self): return security_formatter(self)

class AlpacaOrder(Naming, fields=["term", "tenure", "size", "limit", "stop", "securities"]):
    def __len__(self): return len(self.securities)


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"], headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}


class AlpacaOrderPayload(WebPayload, key="order", fields={"qty": "1", "order_class": "mleg"}, multiple=False, optional=False):
    limit = lambda order: {"limit_price": f"{order.limit:.02f}"} if order.term in (Variables.Markets.Term.LIMIT, Variables.Markets.Term.STOPLIMIT) else {}
    stop = lambda order: {"stop_price": f"{order.stop:.02f}"} if order.term in (Variables.Markets.Term.STOP, Variables.Markets.Term.STOPLIMIT) else {}
    tenure = lambda order: {"time_in_force": tenure_formatter(order)}
    term = lambda order: {"type": term_formatter(order)}
    size = lambda order: {"qty": str(order.size)}

    class Securities(WebPayload, key="securities", locator="legs", fields={}, multiple=True, optional=True):
        security = lambda security: {"symbol": security_formatter(security)}
        quantity = lambda security: {"ratio_qty": quantity_formatter(security)}
        action = lambda security: {"side": action_formatter(security)}


class AlpacaOrderData(WebJSON, key="order", multiple=False, optional=False):
    class Limit(WebJSON.Text, key="limit", locator="limit_price", parser=limit_parser): pass
    class Stop(WebJSON.Text, key="stop", locator="stop_price", parser=stop_parser): pass
    class Tenure(WebJSON.Text, key="tenure", locator="time_in_force", parser=tenure_parser): pass
    class Term(WebJSON.Text, key="term", locator="type", parser=term_parser): pass
    class Size(WebJSON.Text, key="size", locator="qty", parser=np.int32): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        stocks = [security for security in contents["securities"] if security.instrument == Variables.Securities.Instrument.STOCK]
        options = [security for security in contents["securities"] if security.instrument == Variables.Securities.Instrument.OPTION]
        return AlpacaOrder(security=stocks + options, **contents)

    class Securities(WebJSON, key="securities", locator="legs", multiple=True, optional=True):
        class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
        class Expire(WebJSON.Text, key="expire", locator="symbol", parser=expire_parser): pass
        class Instrument(WebJSON.Text, key="instrument", locator="symbol", parser=instrument_parser): pass
        class Option(WebJSON.Text, key="option", locator="symbol", parser=option_parser): pass
        class Position(WebJSON.Text, key="position", locator="side", parser=position_parser): pass
        class Strike(WebJSON.Text, key="strike", locator="symbol", parser=strike_parser): pass
        class Action(WebJSON.Text, key="action", locator="side", parser=action_parser): pass
        class Quantity(WebJSON.Text, key="quantity", locator="ratio_qty", parser=int): pass

        def execute(self, *args, **kwargs):
            contents = super().execute(*args, **kwargs)
            security = Securities(list(map(contents.get, list(Variables.Securities.Security))))
            return AlpacaSecurity(security=security, **contents)


class AlpacaOrderPage(WebJSONPage):
    def execute(self, *args, order, **kwargs):
        assert isinstance(order, AlpacaOrder)
        url = AlpacaOrderURL(*args, **kwargs)
        payload = AlpacaOrderPayload(order, *args, **kwargs)
        self.load(url, *args, payload=dict(payload), **kwargs)
        datas = AlpacaOrderData(self.json, *args, **kwargs)
        contents = datas(*args, **kwargs)
        return contents


class AlpacaOrderUploader(Emptying, Logging, title="Uploaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaOrderPage(*args, **kwargs)

    def execute(self, prospects, *args, **kwargs):
        assert isinstance(prospects, pd.DataFrame)
        if self.empty(prospects): return
        for settlement, order in self.orders(prospects, *args, **kwargs):
            order = self.upload(order, *args, **kwargs)
            spot = - np.round(order.spot, 2).astype(np.float32)
            self.console(f"{str(settlement)}[{len(order):.0f}]")
            cashflow = "expense" if bool(spot <= 0) else "revenue"
            self.console(f"${abs(spot):.2f} {cashflow}", title="Cashflow")

    def upload(self, order, *args, **kwargs):
        order = self.page(*args, order=order, **kwargs)
        assert order.term in (Variables.Markets.Term.MARKET, Variables.Markets.Term.LIMIT)
        return order

    @staticmethod
    def orders(prospects, *args, term, tenure, **kwargs):
        assert term in (Variables.Markets.Term.MARKET, Variables.Markets.Term.LIMIT)
        header = ["strategy", "spot", "size"] + list(Querys.Settlement) + list(map(str, Securities.Options))
        orders = prospects.loc[:, prospects.columns.get_level_values(0).isin(set(header))].droplevel(1, axis=1)
        for index, order in orders.iterrows():
            order = order.dropna(inplace=False)
            price = - np.round(order.spot, 2).astype(np.float32)
            settlement = Querys.Settlement([order.ticker, order.expire])
            function = lambda security: dict(instrument=security.instrument, option=security.option, position=security.position)
            stocks = [AlpacaSecurity(ticker=order.ticker, **function(security)) for security in order.strategy.stocks]
            options = [AlpacaSecurity(ticker=order.ticker, expire=order.expire, strike=order[str(security)], **function(security)) for security in order.strategy.options]
            order = AlpacaOrder(limit=price, stop=None, term=term, tenure=tenure, size=order.size, securities=stocks + options)
            yield settlement, order

    @property
    def page(self): return self.__page


