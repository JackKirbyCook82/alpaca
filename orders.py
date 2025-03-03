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
from webscraping.weburl import WebURL, WebPayload, WebField
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from support.mixins import Emptying, Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


osi_parser = lambda string: OSI(string)
instrument_parser = lambda string: Variables.Securities.Instrument.OPTION if any(list(map(str.isdigit, string))) else Variables.Securities.Instrument.STOCK
ticker_parser = lambda string: osi_parser(string).ticker if instrument_parser(string) == Variables.Securities.Instrument.OPTION else str(string).upper()
expire_parser = lambda string: osi_parser(string).expire if instrument_parser(string) == Variables.Securities.Instrument.OPTION else None
option_parser = lambda string: osi_parser(string).option if instrument_parser(string) == Variables.Securities.Instrument.OPTION else Variables.Securities.Option.EMPTY
strike_parser = lambda string: osi_parser(string).strike if instrument_parser(string) == Variables.Securities.Instrument.OPTION else None
position_parser = lambda string: {"buy": Variables.Securities.Position.LONG, "sell": Variables.Securities.Position.SHORT}[string]
action_parser = lambda string: {"buy": Variables.Markets.Action.BUY, "sell": Variables.Markets.Action.SELL}[string]
tenure_parser = lambda string: {"day": Variables.Markets.Tenure.DAY, "fok": Variables.Markets.Tenure.FILLKILL}[string]
term_parser = lambda string: {"market": Variables.Markets.Terms.MARKET, "limit": Variables.Markets.Terms.LIMIT}[string]
price_parser = lambda string: np.round(float(string), 2).astype(np.float32)

stock_formatter = lambda security: str(security.ticker).upper()
option_formatter = lambda security: str(OSI(security.ticker, security.expire, security.option, security.strike))
security_formatter = lambda security: {Variables.Securities.Instrument.STOCK: stock_formatter, Variables.Securities.Instrument.OPTION: option_formatter}[security.instrument]
quantity_formatter = lambda security: {Variables.Securities.Instrument.STOCK: "100", Variables.Securities.Instrument.OPTION: "1"}[security.instrument]
action_formatter = lambda security: {Variables.Securities.Position.LONG: "buy", Variables.Securities.Position.SHORT: "sell"}[security.position]
limit_formatter = lambda order: {Variables.Markets.Terms.MARKET: None, Variables.Markets.Terms.LIMIT: f"{order.price:.02f}"}[order.term]
tenure_formatter = lambda order: {Variables.Markets.Tenure.DAY: "day", Variables.Markets.Tenure.FILLKILL: "fok"}[order.tenure]
term_formatter = lambda order: {Variables.Markets.Terms.MARKET: "market", Variables.Markets.Terms.LIMIT: "limit"}[order.term]


class AlpacaSecurity(ntuple("Option", "ticker expire instrument option position strike")):
    @classmethod
    def option(cls, ticker, expire, security, strike): return cls(ticker, expire, security.instrument, security.option, security.position, strike)
    @classmethod
    def stock(cls, ticker, security): return cls(ticker, None, security.instrument, security.option, security.position, None)

class AlpacaOrder(ntuple("Order", "term tenure price")):
    def __new__(cls, term, tenure, price, *args, **kwargs): return super().__new__(term, tenure, price)
    def __init__(self, *args, stocks, options, **kwargs):
        self.__options = list(options)
        self.__stocks = list(stocks)

    @property
    def securities(self): return self.stocks + self.options
    @property
    def options(self): return self.__options
    @property
    def stocks(self): return self.__stocks


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"], headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}


class AlpacaOrderPayload(WebPayload, key="order", fields={"qty": "1", "order_class": "mleg", "extended_hours": "false"}, multiple=False, optional=False):
    price = WebField("limit_price", limit_formatter)
    tenure = WebField("time_in_force", tenure_formatter)
    term = WebField("type", term_formatter)

    class Securities(WebPayload, key="securities", locator="legs", fields={}, multiple=True, optional=True):
        security = WebField("symbol", security_formatter)
        quantity = WebField("ratio_qty", quantity_formatter)
        action = WebField("side", action_formatter)


class AlpacaOrderData(WebJSON, key="order", multiple=False, optional=False):
    class Price(WebJSON.Text, key="price", locator="limit_price", parser=price_parser): pass
    class Tenure(WebJSON.Text, key="tenure", locator="time_in_force", parser=tenure_parser): pass
    class Term(WebJSON.Text, key="term", locator="type", parser=term_parser): pass

    def execute(self, *args, **kwargs):
        contents = super().execute(*args, **kwargs)
        stocks = [security for security in contents["securities"] if security.instrument == Variables.Securities.Instrument.STOCK]
        options = [security for security in contents["securities"] if security.instrument == Variables.Securities.Instrument.OPTION]
        arguments = list(map(contents.get, ["term", "tenure", "price"]))
        parameters = dict(stocks=stocks, options=options)
        return AlpacaOrder(*arguments, **parameters)

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
            if security.instrument is Variables.Securities.Instrument.STOCK: return AlpacaSecurity.stock(contents["ticker"], contents["expire"], security, contents["strike"])
            elif security.instrument is Variables.Securities.Instrument.OPTION: return AlpacaSecurity.option(contents["ticker"], security)
            else: pass


class AlpacaOrderPage(WebJSONPage):
    def execute(self, *args, order, **kwargs):
        assert isinstance(order, AlpacaOrder)
        url = AlpacaOrderURL(*args, **kwargs)
        payload = AlpacaOrderPayload(order, *args, **kwargs)
        self.load(url, *args, payload=payload.json, **kwargs)
        data = AlpacaOrderData(self.json, *args, **kwargs)
        contents = data(*args, **kwargs)
        return contents


class AlpacaOrderUploader(Emptying, Logging, title="Uploaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaOrderPage(*args, **kwargs)

    def execute(self, prospects, *args, **kwargs):
        assert isinstance(prospects, pd.DataFrame)
        if self.empty(prospects): return
        for order in self.orders(prospects, *args, **kwargs):
            order = self.page(*args, order=order, **kwargs)

    @staticmethod
    def orders(prospects, *args, term, tenure, **kwargs):
        header = ["strategy", "spot"] + list(Querys.Settlement) + list(map(str, Securities.Options))
        orders = prospects.loc[:, prospects.columns.get_level_values(0).isin(set(header))].droplevel(1, axis=1)
        for index, order in orders.iterrows():
            order = order.dropna(inplace=False)
            price = - np.round(float(order.spot), 2).astype(np.float32)
            stocks = [AlpacaSecurity.stock(order.ticker, security) for security in order.strategy.stocks]
            options = [AlpacaSecurity.option(order.ticker, order.expire, security, order[str(security)]) for security in order.strategy.options]
            order = AlpacaOrder(term, tenure, price, stocks=stocks, options=options)
            yield order

    @property
    def page(self): return self.__page



