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


action_formatter = lambda security: {Variables.Securities.Position.LONG: "buy", Variables.Securities.Position.SHORT: "sell"}[security.position]
tenure_formatter = lambda order: {Variables.Markets.Tenure.DAY: "day", Variables.Markets.Tenure.FILLKILL: "fok"}[order.tenure]
term_formatter = lambda order: {Variables.Markets.Term.MARKET: "market", Variables.Markets.Term.LIMIT: "limit"}[order.term]


class AlpacaSecurity(Naming, ABC, fields=["ticker", "instrument", "option", "position"]):
    def __new__(cls, security, *args, **kwargs):
        security = dict(instrument=security.instrument, option=security.option, position=security.position)
        return super().__new__(cls, *args, **security, **kwargs)

class AlpacaStock(AlpacaSecurity):
    def __str__(self): return str(self.ticker)

class AlpacaOption(AlpacaSecurity, fields=["expire", "strike"]):
    def __str__(self): return str(OSI([self.ticker, self.expire, self.option, self.strike]))

class AlpacaValuation(Naming, fields=["npv"]):
    def __str__(self): return f"${self.npv.min():.0f} -> ${self.npv.max():.0f}"

class AlpacaOrderMeta(RegistryMeta, ABCMeta): pass
class AlpacaOrder(Naming, ABC, fields=["term", "tenure", "limit", "stop", "quantity", "securities"], metaclass=AlpacaOrderMeta): pass
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
    quantity = lambda order: {"qty": str(order.quantity)}

    class Securities(WebPayload, key="securities", locator="legs", fields={"ratio_qty": "1"}, multiple=True, optional=True):
        action = lambda security: {"side": action_formatter(security)}
        security = lambda security: {"symbol": str(security)}


class AlpacaOrderPage(WebJSONPage):
    def execute(self, *args, order, **kwargs):
        url = AlpacaOrderURL(*args, **kwargs)
        payload = AlpacaOrderPayload(order, *args, **kwargs)

        print(url)
        print(payload)
        return

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

        for order, valuation in self.calculator(prospects, *args, **kwargs):
            self.upload(order, *args, **kwargs)
            securities = ", ".join(list(map(str, order.securities)))
            self.console(f"{str(securities)}[{order.quantity:.0f}] @ {str(valuation)}")

        raise Exception()

    def upload(self, order, *args, **kwargs):
        assert order.term in (Variables.Markets.Term.MARKET, Variables.Markets.Term.LIMIT)
        parameters = dict(order=order, api=self.api)
        self.page(*args, **parameters, **kwargs)

    @staticmethod
    def calculator(prospects, *args, term, tenure, **kwargs):
        assert term in (Variables.Markets.Term.MARKET, Variables.Markets.Term.LIMIT)
        for index, prospect in prospects.iterrows():
            strategy, quantity = prospect[["strategy", "quantity"]].droplevel(1).values
            settlement = prospect[list(Querys.Settlement)].droplevel(1).to_dict()
            options = prospect[list(map(str, Securities.Options))].droplevel(1).to_dict()
            options = {Securities.Options[option]: strike for option, strike in options.items() if not np.isnan(strike)}
            stocks = {Securities.Stocks[stock] for stock in strategy.stocks}
            breakeven = prospect[("spot", Variables.Scenario.BREAKEVEN)]
            current = prospect[("spot", Variables.Scenario.CURRENT)]
            assert current >= breakeven and quantity >= 1
            options = [AlpacaOption(security, strike=strike, **settlement) for security, strike in options.items()]
            stocks = [AlpacaStock(security, **settlement) for security in stocks]
            valuation = AlpacaValuation(npv=prospect.xs("npv", axis=0, level=0, drop_level=True))
            order = AlpacaOrder[strategy](securities=stocks + options, term=term, tenure=tenure, limit=-breakeven, stop=None, quantity=1)
            yield order, valuation

    @property
    def page(self): return self.__page
    @property
    def api(self): return self.__api





