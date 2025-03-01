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
from support.decorators import ValueDispatcher
from support.mixins import Emptying, Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"], headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}


class AlpacaOrderPayload(WebPayload, parameters={"qty": "1", "order_class": "mleg", "extended_hours": "false"}, multiple=False, optional=False):
    terms = {Variables.Markets.Terms.MARKET: "market", Variables.Markets.Terms.LIMIT: "limit"}
    tenures = {Variables.Markets.Tenure.DAY: "day", Variables.Markets.Tenure.FILLKILL: "fok"}

    @ValueDispatcher(locator="term")
    def price(self, *args, term, **kwargs): raise ValueError(term)
    @price.register(Variables.Markets.Terms.LIMIT)
    def limit(self, *args, price, **kwargs): return {"limit_price": f"{price:.02f}"}
    @price.register(Variables.Markets.Terms.MARKET)
    def market(self, *args, **kwargs): return {}

    def tenure(self, *args, tenure, **kwargs): return {"time_in_force": self.tenures[tenure]}
    def term(self, *args, term, **kwargs): return {"type": self.terms[term]}
    def execute(self, *args, **kwargs):
        tenure = self.tenure(*args, **kwargs)
        term = self.term(*args, **kwargs)
        price = self.price(*args, **kwargs)
        return dict(tenure) | dict(term) | dict(price)

    class AlpacaSecurityPayload(WebPayload, key="securities", locator="legs", multiple=True, optional=False):
        quantities = {Variables.Securities.Instrument.STOCK: "100", Variables.Securities.Instrument.OPTION: "1"}
        actions = {Variables.Securities.Position.LONG: "buy", Variables.Securities.Position.SHORT: "sell"}

        @ValueDispatcher(locator="security")
        def security(self, *args, security, **kwargs): raise ValueError(security)
        @security.register(*list(Securities.Options))
        def option(self, *args, security, ticker, expire, strike, **kwargs): return {"symbol": str(OSI([ticker, expire, security.option, strike]))}
        @security.register(*list(Securities.Stocks))
        def stock(self, *args, ticker, **kwargs): return {"symbol": str(ticker).upper()}

        def quantity(self, *args, security, **kwargs): return {"ratio_qty": self.quantities[security.instrument]}
        def action(self, *args, security, **kwargs): return {"side": self.actions[security.position]}
        def execute(self, *args, **kwargs):
            security = self.security(*args, **kwargs)
            quantity = self.quantity(*args, **kwargs)
            action = self.action(*args, **kwargs)
            return dict(security) | dict(quantity) | dict(action)


class AlpacaOrderData(WebJSON, multiple=False, optional=False):
    class Revenue(WebJSON.Text, locator="", key="revenue", parser=): pass
    class Expense(WebJSON.Text, locator="", key="expense", parser=): pass
    class Tenure(WebJSON.Text, locator="", key="tenure", parser=): pass
    class Term(WebJSON.Text, locator="", key="term", parser=): pass
    class Security(WebJSON, locator="", multiple=True, optional=True):
        class Ticker(WebJSON.Text, locator="", key="ticker", parser=): pass
        class Expire(WebJSON.Text, locator="", key="expire", parser=): pass
        class Instrument(WebJSON.Text, locator="", key="instrument", parser=): pass
        class Option(WebJSON.Text, locator="", key="option", parser=): pass
        class Position(WebJSON.Text, locator="", key="position", parser=): pass
        class Action(WebJSON.Text, locator="", key="action", parser=): pass
        class Quantity(WebJSON.Text, locator="", key="quantity", parser=): pass


class AlpacaOrderPage(WebJSONPage):
    def execute(self, *args, order, **kwargs):
        assert isinstance(order, dict)
        url = AlpacaOrderURL(*args, **kwargs)
        payload = AlpacaOrderPayload(order, *args, **kwargs)
        self.load(url, *args, payload=payload.json, **kwargs)
        data = AlpacaOrderData(self.json, *args, **kwargs)
        contents = data(*args, **kwargs)
        return contents


class AlpacaStock(ntuple("Stock", "ticker security")): pass
class AlpacaOption(ntuple("Option", "ticker expire security strike")): pass
class AlpacaOrder(ntuple("Order", "term tenure cash stocks options")): pass


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
            stocks = [AlpacaStock(order.ticker, stock) for stock in order.strategy.stocks]
            options = [AlpacaOption(order.ticker, order.expire, option, order[str(option)]) for option in order.strategy.options]
            order = AlpacaOrder(term, tenure, price, stocks, options)
            yield order

    @property
    def page(self): return self.__page



