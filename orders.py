# -*- coding: utf-8 -*-
"""
Created on Tues Feb 18 2025
@name:   Aplaca Order Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd

from finance.variables import Querys, Variables, Securities, OSI
from webscraping.weburl import WebURL, WebPayload
from webscraping.webpages import WebJSONPage
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


class AlpacaOrderPayload(WebPayload, arguments=["price", "tenure", "term"], parameters={"qty": 1, "order_class": "mleg"}, multiple=False, optional=False):
    terms = {Variables.Markets.Terms.MARKET: "market", Variables.Markets.Terms.LIMIT: "limit"}
    tenures = {Variables.Markets.Tenure.DAY: "day", Variables.Markets.Tenure.FILLKILL: "fok"}

    @ValueDispatcher(locator="term")
    def price(cls, *args, term, **kwargs): raise ValueError(term)
    @price.register(Variables.Markets.Terms.LIMIT)
    def limit(cls, *args, price, **kwargs): return {"limit_price": f"{price:.02}"}
    @price.register(Variables.Markets.Terms.MARKET)
    def market(cls, *args, **kwargs): return {}

    def tenure(cls, *args, tenure, **kwargs): return {"time_in_force": cls.tenures[tenure]}
    def term(cls, *args, term, **kwargs): return {"type": cls.terms[term]}

    class AlpacaLegPayload(WebPayload, key="securities", locator="legs", arguments=["security", "quantity", "action"], multiple=True, optional=False):
        quantities = {Variables.Securities.Instrument.STOCK: 100, Variables.Securities.Instrument.OPTION: 1}
        actions = {Variables.Securities.Position.LONG: "buy", Variables.Securities.Position.SHORT: "sell"}

        @ValueDispatcher(locator="security")
        def security(cls, *args, security, **kwargs): raise ValueError(security)
        @security.register(Securities.Options)
        def option(cls, *args, security, ticker, expire, strike, **kwargs): return {"symbol": OSI([ticker, expire, security.option, strike])}
        @security.register(Securities.Stocks)
        def stock(cls, *args, ticker, **kwargs): return {"symbol": str(ticker).upper()}

        def quantity(cls, *args, security, **kwargs): return {"ratio_qty": cls.quantities[security.instrument]}
        def action(cls, *args, security, **kwargs): return {"side": cls.actions[security.poistion]}


class AlpacaOrderPage(WebJSONPage):
    def execute(self, *args, order, **kwargs):
        assert isinstance(order, dict)
        url = self.AlpacaOrderURL(*args, **kwargs)
        payload = self.AlpacaOrderPayload(*args, order=order, **kwargs)
        self.load(url, *args, payload=payload, **kwargs)


class AlpacaOrderUploader(Emptying, Logging, title="Uploaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaOrderPage(*args, **kwargs)

    def execute(self, prospects, *args, **kwargs):
        assert isinstance(prospects, pd.DataFrame)
        if self.empty(prospects): return
        for order in self.orders(prospects, *args, **kwargs):
            self.page(*args, order=order, **kwargs)

    @staticmethod
    def orders(prospects, *args, term, tenure, **kwargs):
        header = ["strategy", "spot"] + list(Querys.Settlement) + list(map(str, Securities.Options))
        orders = prospects.loc[:, prospects.columns.get_level_values(0).isin(set(header))].droplevel(1, axis=1)
        for index, order in orders.iterrows():
            order = order.dropna(inplace=False)
            price = - np.round(float(order.spot), 2).astype(np.float32)
            stocks = [dict(ticker=order.ticker, security=stock) for stock in order.strategy.stocks]
            options = [dict(ticker=order.ticker, expire=order.expire, security=option, strike=order[option]) for option in order.strategy.options]
            yield dict(term=term, tenure=tenure, price=price, securities=stocks + options)

    @property
    def page(self): return self.__page



