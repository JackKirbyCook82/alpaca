# -*- coding: utf-8 -*-
"""
Created on Tues Feb 18 2025
@name:   Aplaca Order Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from abc import ABC, ABCMeta

from finance.variables import Querys, Variables, Securities, Strategies, OSI
from webscraping.weburl import WebURL, WebPayload
from webscraping.webpages import WebJSONPage
from support.mixins import Emptying, Logging, Naming
from support.meta import RegistryMeta

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


option_formatter = lambda security: str(OSI([security.ticker, security.expire, security.option, security.strike]))
action_formatter = lambda security: {Variables.Securities.Position.LONG: "buy", Variables.Securities.Position.SHORT: "sell"}[security.position]
tenure_formatter = lambda order: {Variables.Markets.Tenure.DAY: "day", Variables.Markets.Tenure.FILLKILL: "fok"}[order.tenure]
term_formatter = lambda order: {Variables.Markets.Term.MARKET: "market", Variables.Markets.Term.LIMIT: "limit"}[order.term]


class AlpacaValuation(Naming, fields=["npv"]):
    def __str__(self): return "|".join([f"${value:.0f}" for value in self.npv.values()])
    def __new__(cls, prospect, *args, **kwargs):
        npv = prospect.xs("npv", axis=0, level=0, drop_level=True)
        return super().__new__(cls, *args, npv=npv, **kwargs)


class AlpacaSecurity(Naming, ABC, fields=["instrument", "option", "position"]): pass
class AlpacaStock(AlpacaSecurity, fields=["ticker"]): pass
class AlpacaOption(AlpacaSecurity, fields=["ticker", "expire", "strike"]):
    def __str__(self): return str(OSI([self.ticker, self.expire, self.option, self.strike]))


class AlpacaOrderMeta(RegistryMeta, ABCMeta):
    def __call__(cls, *args, strategy, settlement, strikes, **kwargs):
        options = {option: strikes[option] for option in strategy.options}
        options = [dict(instrument=security.instrument, option=security.option, position=security.position, strike=strike) for security, strike in options.items()]
        options = [AlpacaOption(**settlement, **option) for option in options]
        stocks = [dict(instrument=security.instrument, option=security.option, position=security.position) for security in strategy.stocks]
        stocks = [AlpacaStock(**settlement, **stock) for stock in stocks]
        parameters = dict(stocks=stocks, options=options)
        instance = super(AlpacaOrderMeta, cls[strategy]).__call__(*args, **parameters, **kwargs)
        return instance

class AlpacaOrder(Naming, ABC, fields=["size", "term", "tenure", "limit", "stop", "quantity", "stocks", "options"], metaclass=AlpacaOrderMeta):
    def __new__(cls, *args, spot, breakeven, quantity, **kwargs):
        assert breakeven <= spot and quantity >= 1
        limit = - np.round(breakeven, 2).astype(np.float32)
        parameters = dict(limit=limit, stop=None, quantity=1)
        return super().__new__(cls, *args, **parameters, **kwargs)

class VerticalPutOrder(AlpacaOrder, register=Strategies.Verticals.Put): pass
class VerticalCallOrder(AlpacaOrder, register=Strategies.Verticals.Call): pass


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"], headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, api, **kwargs):
        assert isinstance(api, tuple)
        return {"APCA-API-KEY-ID": str(api.identity), "APCA-API-SECRET-KEY": str(api.code)}


class AlpacaOrderPayload(WebPayload, key="order", fields={"order_class": "mleg"}, multiple=False, optional=False):
    limit = lambda order: {"limit_price": f"{order.limit:.02f}"} if order.term in (Variables.Markets.Term.LIMIT, Variables.Markets.Term.STOPLIMIT) else {}
    stop = lambda order: {"stop_price": f"{order.stop:.02f}"} if order.term in (Variables.Markets.Term.STOP, Variables.Markets.Term.STOPLIMIT) else {}
    tenure = lambda order: {"time_in_force": tenure_formatter(order)}
    term = lambda order: {"type": term_formatter(order)}
    size = lambda order: {"qty": str(order.quantity)}

    class Options(WebPayload, key="options", locator="legs", fields={"ratio_qty": "1"}, multiple=True, optional=True):
        option = lambda security: {"symbol": option_formatter(security)}
        action = lambda security: {"side": action_formatter(security)}


class AlpacaOrderPage(WebJSONPage):
    def execute(self, *args, order, **kwargs):
        url = AlpacaOrderURL(*args, **kwargs)
        payload = AlpacaOrderPayload(order, *args, **kwargs)
        self.load(url, *args, payload=dict(payload), **kwargs)


class AlpacaOrderUploader(Emptying, Logging, title="Uploaded"):
    def __init__(self, *args, api, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaOrderPage(*args, **kwargs)
        self.__api = api

    def execute(self, prospects, *args, **kwargs):
        assert isinstance(prospects, pd.DataFrame)
        if self.empty(prospects): return

        print(prospects)
        raise Exception()

        for order, valuation in self.calculator(prospects, *args, **kwargs):
            self.upload(order, *args, **kwargs)
            securities = ", ".join(list(map(str, order.securities)))
            self.console(f"{str(securities)}|{str(order.valuation)}[{order.quantity:.0f}]")

    def upload(self, order, *args, **kwargs):
        assert order.term in (Variables.Markets.Term.MARKET, Variables.Markets.Term.LIMIT)
        parameters = dict(order=order, api=self.api)
        self.page(*args, **parameters, **kwargs)

    @staticmethod
    def calculator(prospects, *args, term, tenure, **kwargs):
        assert term in (Variables.Markets.Term.MARKET, Variables.Markets.Term.LIMIT)
        for index, prospect in prospects.iterrows():
            settlement = prospect[list(Querys.Settlement)].droplevel(1).to_dict()
            options = prospect[list(map(str, Securities.Options))].droplevel(1).to_dict()
            strikes = {Securities.Options[option]: strike for option, strike in options.items() if not np.isnan(strike)}
            order = prospect[["strategy", "spot", "breakeven", "quantity"]].droplevel(1).to_dict()
            order = dict(order) | dict(term=term, tenure=tenure) | dict(settlement=settlement, strikes=strikes)
            try: order = AlpacaOrder(*args, **order, **kwargs)
            except KeyError: continue
            valuation = AlpacaValuation(prospect, *args, **kwargs)
            yield order, valuation

    @property
    def page(self): return self.__page
    @property
    def api(self): return self.__api





