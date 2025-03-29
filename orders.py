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


class AlpacaSecurity(Naming, fields=["ticker", "expire", "instrument", "option", "position", "strike"]):
    def __str__(self): return str(OSI([self.ticker, self.expire, self.option, self.strike]))
    def __new__(cls, *args, settlement, security, strike, **kwargs):
        return super().__new__(cls, strike=strike, **dict(settlement.items()), **dict(security.items()))

class AlpacaValuation(Naming, metaclass=RegistryMeta): pass
class AlpacaArbitrage(AlpacaValuation, fields=["apy", "npv"], register=Variables.Valuations.Valuation.ARBITRAGE):
    def __str__(self): return f"{self.formatter(self.apy * 100, prefix='%')}, {self.formatter(self.npv, prefix='$')}"
    def __new__(cls, prospect, *args, **kwargs):
        apy = prospect[("apy", Variables.Valuations.Scenario.MINIMUM)]
        npv = prospect[("npv", Variables.Valuations.Scenario.MINIMUM)]
        return super().__new__(cls, *args, apy=apy, npv=npv, **kwargs)

    @staticmethod
    def formatter(value, prefix):
        if not np.isfinite(value): return "InF"
        elif value > 10 ** 12: return "EsV"
        elif value > 10 ** 9: return f"{prefix}{value / (10 ** 9):.2f}B"
        elif value > 10 ** 6: return f"{prefix}{value / (10 ** 6):.2f}M"
        elif value > 10 ** 3: return f"{prefix}{value / (10 ** 3):.2f}K"
        else: return f"{prefix}{value:.2f}"


class AlpacaOrder(Naming, fields=["size", "term", "tenure", "limit", "stop"]):
    def __len__(self): return len(self.securities)
    def __init__(self, *args, securities, valuation, **kwargs):
        self.securities = list(securities)
        self.valuation = valuation


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
    size = lambda order: {"qty": str(order.size)}

    class Securities(WebPayload, key="securities", locator="legs", fields={"ratio_qty": "1"}, multiple=True, optional=True):
        option = lambda security: {"symbol": option_formatter(security)}
        action = lambda security: {"side": action_formatter(security)}


class AlpacaOrderPage(WebJSONPage):
    def execute(self, *args, order, **kwargs):
        assert isinstance(order, AlpacaOrder)
        url = AlpacaOrderURL(*args, **kwargs)
        payload = AlpacaOrderPayload(order, *args, **kwargs)

        print(url)
        print(payload)
        raise Exception()

        self.load(url, *args, payload=dict(payload), **kwargs)


class AlpacaOrderUploader(Emptying, Logging, title="Uploaded"):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__page = AlpacaOrderPage(*args, **kwargs)

    def execute(self, orders, *args, **kwargs):
        assert isinstance(orders, pd.DataFrame)
        if self.empty(orders): return
        for order in self.orders(orders, *args, **kwargs):
            self.upload(order, *args, **kwargs)
            securities = ", ".join(list(map(str, order.securities)))
            self.console(f"{str(securities)}[{str(order.valuation)}, {order.size:.0f}]")

    def upload(self, order, *args, **kwargs):
        assert order.term in (Variables.Markets.Term.MARKET, Variables.Markets.Term.LIMIT)
        self.page(*args, order=order, **kwargs)

    @staticmethod
    def orders(orders, *args, term, tenure, **kwargs):
        assert term in (Variables.Markets.Term.MARKET, Variables.Markets.Term.LIMIT)
        header = ["strategy", "valuation"] + list(Querys.Settlement) + list(map(str, Securities.Options)) + ["spot", "size"]
        for index, order in orders.iterrows():
            limit = - np.round(order[("spot", "")] - order[("npv", Variables.Valuations.Scenario.MINIMUM)], 2).astype(np.float32)
            series = order[header].droplevel(1)
            settlement = Querys.Settlement(series[list(Querys.Settlement)].to_dict())
            securities = series[list(map(str, Securities.Options))].to_dict()
            securities = {Securities[security]: strike for security, strike in securities.items() if not np.isnan(strike)}
            size = + np.round(series["size"], 1).astype(np.int32)
            valuation = AlpacaValuation[series.valuation](order)
            securities = [AlpacaSecurity(settlement=settlement, security=security, strike=strike) for security, strike in securities.items()]
            yield AlpacaOrder(size=size, limit=limit, stop=None, term=term, tenure=tenure, securities=securities, valuation=valuation)

    @property
    def page(self): return self.__page


