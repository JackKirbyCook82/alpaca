# -*- coding: utf-8 -*-
"""
Created on Tues Feb 18 2025
@name:   Aplaca Order Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from abc import ABC, ABCMeta

from finance.concepts import Querys, Concepts, Securities, Strategies, OSI
from webscraping.weburl import WebURL, WebPayload
from webscraping.webpages import WebJSONPage
from support.mixins import Emptying, Logging, Naming
from support.meta import RegistryMeta

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


action_formatter = lambda security: {Concepts.Securities.Position.LONG: "buy", Concepts.Securities.Position.SHORT: "sell"}[security.position]
tenure_formatter = lambda order: {Concepts.Markets.Tenure.DAY: "day", Concepts.Markets.Tenure.FILLKILL: "fok"}[order.tenure]
term_formatter = lambda order: {Concepts.Markets.Term.MARKET: "market", Concepts.Markets.Term.LIMIT: "limit"}[order.term]


class AlpacaSecurity(Naming, ABC, fields=["ticker", "instrument", "option", "position"]):
    def __new__(cls, security, *args, **kwargs):
        security = dict(instrument=security.instrument, option=security.option, position=security.position)
        return super().__new__(cls, *args, **security, **kwargs)

class AlpacaOption(AlpacaSecurity, fields=["expire", "strike"]):
    def __str__(self): return str(OSI([self.ticker, self.expire, self.option, self.strike]))

class AlpacaStock(AlpacaSecurity):
    def __str__(self): return str(self.ticker)

class AlpacaValuation(Naming, fields=["npv"]):
    def __str__(self): return f"${self.npv:.0f}"

class AlpacaOrderMeta(RegistryMeta, ABCMeta): pass
class AlpacaOrder(Naming, ABC, fields=["term", "tenure", "limit", "stop", "quantity", "securities"], metaclass=AlpacaOrderMeta): pass
class VerticalPutOrder(AlpacaOrder, register=Strategies.Verticals.Put): pass
class VerticalCallOrder(AlpacaOrder, register=Strategies.Verticals.Call): pass


class AlpacaOrderURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "orders"], headers={"accept": "application/json", "content-type": "application/json"}):
    @staticmethod
    def headers(*args, webapi, **kwargs):
        assert isinstance(webapi, tuple)
        return {"APCA-API-KEY-ID": str(webapi.identity), "APCA-API-SECRET-KEY": str(webapi.code)}


class AlpacaOrderPayload(WebPayload, key="order", fields={"order_class": "mleg"}, multiple=False, optional=False):
    limit = lambda order: {"limit_price": f"{order.limit:.02f}"} if order.term in (Concepts.Markets.Term.LIMIT, Concepts.Markets.Term.STOPLIMIT) else {}
    stop = lambda order: {"stop_price": f"{order.stop:.02f}"} if order.term in (Concepts.Markets.Term.STOP, Concepts.Markets.Term.STOPLIMIT) else {}
    tenure = lambda order: {"time_in_force": tenure_formatter(order)}
    term = lambda order: {"type": term_formatter(order)}
    quantity = lambda order: {"qty": str(order.quantity)}

    class Securities(WebPayload, key="securities", locator="legs", fields={"ratio_qty": "1"}, multiple=True, optional=True):
        action = lambda security: {"side": action_formatter(security)}
        security = lambda security: {"symbol": str(security)}


class AlpacaOrderPage(WebJSONPage):
    def __init__(self, *args, webapi, **kwargs):
        super().__init__(*args, **kwargs)
        self.__webapi = webapi

    def execute(self, *args, order, **kwargs):
        url = AlpacaOrderURL(*args, webapi=self.webapi, **kwargs)
        payload = AlpacaOrderPayload(order, *args, **kwargs)
        self.load(url, *args, payload=payload.json, **kwargs)

    @property
    def webapi(self): return self.__webapi


class AlpacaOrderUploader(Emptying, Logging, title="Uploaded"):
    def __init__(self, *args, source, **kwargs):
        super().__init__(*args, **kwargs)
        page = AlpacaOrderPage(*args, source=source, **kwargs)
        self.__page = page

    def execute(self, prospects, /, **kwargs):
        assert isinstance(prospects, pd.DataFrame)
        if self.empty(prospects): return

        print(prospects)
        return

        if "quantity" not in prospects.columns: prospects["quantity"] = 1
        if "priority" not in prospects.columns: prospects["priority"] = prospects["npv"]
        prospects = prospects.sort_values("priority", axis=0, ascending=False, inplace=False, ignore_index=False)
        prospects = prospects.reset_index(drop=True, inplace=False)
        for order, valuation in self.calculator(prospects, **kwargs):
            self.upload(order, **kwargs)
            securities = ", ".join(list(map(str, order.securities)))
            self.console(f"{str(securities)}[{order.quantity:.0f}] @ {str(valuation)}")

    def upload(self, order, *args, **kwargs):
        assert order.term in (Concepts.Markets.Term.MARKET, Concepts.Markets.Term.LIMIT)
        self.page(*args, order=order, **kwargs)

    @staticmethod
    def calculator(prospects, *args, term, tenure, **kwargs):
        assert term in (Concepts.Markets.Term.MARKET, Concepts.Markets.Term.LIMIT)
        for index, prospect in prospects.iterrows():
            strategy, quantity = prospect[["strategy", "quantity"]].values
            spot, breakeven = prospect[["spot", "breakeven"]].values
            settlement = prospect[list(Querys.Settlement)].to_dict()
            options = prospect[list(map(str, Securities.Options))].to_dict()
            options = {Securities.Options[option]: strike for option, strike in options.items() if not np.isnan(strike)}
            stocks = {Securities.Stocks(stock) for stock in strategy.stocks}
            assert spot >= breakeven and quantity >= 1
            options = [AlpacaOption(security, strike=strike, **settlement) for security, strike in options.items()]
            stocks = [AlpacaStock(security, **settlement) for security in stocks]
            valuation = AlpacaValuation(npv=prospect["npv"])
            try: order = AlpacaOrder[strategy](securities=stocks + options, term=term, tenure=tenure, limit=-breakeven, stop=None, quantity=quantity)
            except KeyError: continue
            yield order, valuation

    @property
    def page(self): return self.__page



