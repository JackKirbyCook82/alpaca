# -*- coding: utf-8 -*-
"""
Created on Tues Feb 18 2025
@name:   Aplaca Order Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from collections import namedtuple as ntuple

from finance.variables import Querys, Variables, Securities, OSI
from webscraping.weburl import WebURL, WebPayload
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from support.mixins import Emptying, Logging

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
position_parser = lambda string: {"buy": Variables.Securities.Position.LONG, "sell": Variables.Securities.Position.SHORT}[string]
action_parser = lambda string: {"buy": Variables.Markets.Action.BUY, "sell": Variables.Markets.Action.SELL}[string]
tenure_parser = lambda string: {"day": Variables.Markets.Tenure.DAY, "fok": Variables.Markets.Tenure.FILLKILL}[string]
term_parser = lambda string: {"market": Variables.Markets.Terms.MARKET, "limit": Variables.Markets.Terms.LIMIT}[string]
spot_parser = lambda string: - np.round(float(string), 2).astype(np.float32)
size_parser = lambda string: int(string)

stock_formatter = lambda security: str(security.ticker).upper()
option_formatter = lambda security: str(OSI([security.ticker, security.expire, security.option, security.strike]))
security_formatter = lambda security: {Variables.Securities.Instrument.STOCK: stock_formatter, Variables.Securities.Instrument.OPTION: option_formatter}[security.instrument](security)
quantity_formatter = lambda security: {Variables.Securities.Instrument.STOCK: "100", Variables.Securities.Instrument.OPTION: "1"}[security.instrument]
action_formatter = lambda security: {Variables.Securities.Position.LONG: "buy", Variables.Securities.Position.SHORT: "sell"}[security.position]
tenure_formatter = lambda order: {Variables.Markets.Tenure.DAY: "day", Variables.Markets.Tenure.FILLKILL: "fok"}[order.tenure]
term_formatter = lambda order: {Variables.Markets.Terms.MARKET: "market", Variables.Markets.Terms.LIMIT: "limit"}[order.term]
size_formatter = lambda order: str(order.size)


class AlpacaSecurity(ntuple("Option", "ticker expire instrument option position strike")):
    def __str__(self): return security_formatter(self)
    def __new__(cls, security, *args, ticker, expire=None, strike=None, **kwargs):
        return super().__new__(cls, ticker, expire, security.instrument, security.option, security.position, strike)

class AlpacaOrder(ntuple("Order", "term tenure spot size stocks options")):
    def __len__(self): return len(self.securities)

    @property
    def securities(self): return self.stocks + self.options


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"], headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}


class AlpacaOrderPayload(WebPayload, key="order", fields={"qty": "1", "order_class": "mleg", "extended_hours": "false"}, multiple=False, optional=False):
    limit = lambda order: {"limit_price": f"{-order.spot:.02f}"} if order.term == Variables.Markets.Terms.LIMIT else {}
    tenure = lambda order: {"time_in_force": tenure_formatter(order)}
    term = lambda order: {"type": term_formatter(order)}
    size = lambda order: {"qty": size_formatter(order)}

    class Securities(WebPayload, key="securities", locator="legs", fields={}, multiple=True, optional=True):
        security = lambda security: {"symbol": security_formatter(security)}
        quantity = lambda security: {"ratio_qty": quantity_formatter(security)}
        action = lambda security: {"side": action_formatter(security)}


class AlpacaOrderData(WebJSON, key="order", multiple=False, optional=False):
    class Spot(WebJSON.Text, key="spot", locator="limit_price", parser=spot_parser): pass
    class Tenure(WebJSON.Text, key="tenure", locator="time_in_force", parser=tenure_parser): pass
    class Term(WebJSON.Text, key="term", locator="type", parser=term_parser): pass
    class Size(WebJSON.Text, key="size", locator="qty", parser=size_parser): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        stocks = [security for security in contents["securities"] if security.instrument == Variables.Securities.Instrument.STOCK]
        options = [security for security in contents["securities"] if security.instrument == Variables.Securities.Instrument.OPTION]
        return AlpacaOrder(contents["term"], contents["tenure"], contents["price"], contents["size"], stocks, options)

    class Securities(WebJSON, key="securities", locator="mlegs", multiple=True, optional=True):
        class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
        class Expire(WebJSON.Text, key="expire", locator="symbol", parser=expire_parser): pass
        class Instrument(WebJSON.Text, key="instrument", locator="symbol", parser=instrument_parser): pass
        class Option(WebJSON.Text, key="option", locator="symbol", parser=option_parser): pass
        class Position(WebJSON.Text, key="position", locator="symbol", parser=position_parser): pass
        class Strike(WebJSON.Text, key="strike", locator="symbol", parser=strike_parser): pass
        class Action(WebJSON.Text, key="action", locator="side", parser=action_parser): pass
        class Quantity(WebJSON.Text, key="quantity", locator="ratio_qty", parser=int): pass

        def execute(self, *args, **kwargs):
            contents = super().execute(*args, **kwargs)
            security = Securities(list(map(contents.get, list(Variables.Securities.Security))))
            return AlpacaSecurity(security, ticker=contents["ticker"], expire=contents["expire"], strike=contents["strike"])


class AlpacaOrderPage(WebJSONPage):
    def __init_subclass__(cls, *args, **kwargs):
        cls.__payload__ = AlpacaOrderPayload
        cls.__data__ = AlpacaOrderData
        cls.__url__ = AlpacaOrderURL

    def execute(self, *args, order, **kwargs):
        assert isinstance(order, AlpacaOrder)
        url = self.url(*args, **kwargs)
        payload = self.payload(order, *args, **kwargs)
        self.load(url, *args, payload=payload.json, **kwargs)
        data = self.data(self.json, *args, **kwargs)
        contents = data(*args, **kwargs)
        return contents

    @property
    def payload(self): return type(self).__payload__
    @property
    def data(self): return type(self).__data__
    @property
    def url(self): return type(self).__url__


class AlpacaOrderUploader(Emptying, Logging, title="Uploaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaOrderPage(*args, **kwargs)

    def execute(self, prospects, *args, **kwargs):
        assert isinstance(prospects, pd.DataFrame)
        if self.empty(prospects): return
        for settlement, order in self.orders(prospects, *args, **kwargs):
            order = self.page(*args, order=order, **kwargs)
            self.console(f"{str(settlement)}[{len(order):.0f}]")
            cashflow = "expense" if bool(order.spot <= 0) else "revenue"
            self.console(f"${abs(order.spot):.2f} {cashflow}", title="Cashflow")

    @staticmethod
    def orders(prospects, *args, term, tenure, **kwargs):
        header = ["strategy", "spot", "size"] + list(Querys.Settlement) + list(map(str, Securities.Options))
        orders = prospects.loc[:, prospects.columns.get_level_values(0).isin(set(header))].droplevel(1, axis=1)
        for index, order in orders.iterrows():
            order = order.dropna(inplace=False)
            settlement = Querys.Settlement([order.ticker, order.expire])
            stocks = [AlpacaSecurity(security, ticker=order.ticker) for security in order.strategy.stocks]
            options = [AlpacaSecurity(security, ticker=order.ticker, expire=order.expire, strike=order[str(security)]) for security in order.strategy.options]
            order = AlpacaOrder(term, tenure, order.spot, order.size, stocks, options)
            yield settlement, order

    @property
    def page(self): return self.__page



