# -*- coding: utf-8 -*-
"""
Created on Sat Sept 26 2026
@name:   Alpaca Tick Objects
@author: Jack Kirby Cook
@file:   alpaca/ticks.py

"""

import pandas as pd
from dataclasses import asdict
from abc import ABC, abstractmethod

from alpaca.website import AlpacaDownloadURL, AlpacaDownloadPage, AlpacaDownloader, AlpacaParsers
from finance.enumerations import Price, Instrument
from finance.osi import OSI
from webscraping.webdatas import WebJSON

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockTicksDownloader", "AlpacaOptionTicksDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaTicksURL(AlpacaDownloadURL, ABC, domain="https://data.alpaca.markets", parameters={"limit": 10000}):
    def parameters(self, *args, **kwargs):
        products = self.products(*args, **kwargs)
        history = self.history(*args, **kwargs)
        pagination = self.pagination(*args, **kwargs)
        return products | history | pagination

    @staticmethod
    @abstractmethod
    def products(*args, products, **kwargs): pass


class AlpacaStockTicksURL(AlpacaTicksURL, path=["v2", "stocks", "trades"], parameters={"feed": "sip"}):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

class AlpacaOptionTicksURL(AlpacaTicksURL, path=["v1beta1", "options", "trades"]):
    @staticmethod
    def products(*args, products, **kwargs): return {"symbols": ",".join([str(OSI(product)) for product in products])}


class AlpacaTicksData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=AlpacaParsers.pagination, optional=True): pass


class AlpacaTicksPage(AlpacaDownloadPage, ABC):
    def __call__(self, *args, **kwargs):
        records = self.execute(*args, **kwargs)
        if not records: return None
        bars = pd.DataFrame.from_records(records)
        return bars

    def execute(self, *args, price, pagination=None, **kwargs):
        assert price == Price.TRADE
        url = self.url(*args, pagination=pagination, **kwargs)
        json = self.load(url, *args, **kwargs)
        records = self.records(json["trades"], *args, **kwargs)
        data = self.data(json, *args, **kwargs)
        pagination = data["pagination"](*args, **kwargs)
        if not bool(pagination): return list(records)
        else: return list(records) + self.execute(*args, pagination=pagination, **kwargs)


class AlpacaStockTicksPage(AlpacaTicksPage, url=AlpacaStockTicksURL, data=AlpacaTicksData, instrument=Instrument.STOCK): pass
class AlpacaOptionTicksPage(AlpacaTicksPage, url=AlpacaOptionTicksURL, data=AlpacaTicksData, instrument=Instrument.OPTION): pass


class AlpacaTicksDownloader(AlpacaDownloader):
    def __call__(self, products, /, price, **kwargs):
        assert price == Price.TRADE
        if not isinstance(products, list): products = [products]
        trades = list(self.downloader(products, price=price, **kwargs))
        if not trades: return None
        trades = pd.concat(list(trades), axis=0)
        trades = self.parser(trades, **kwargs)
        return trades

    def downloader(self, products, /, **kwargs):
        products = [products[index:index + 100] for index in range(0, len(products), 100)]
        for products in products:
            scope = self.scope(products)
            trades = self.page(products=products, **kwargs)
            if trades is None or bool(trades.empty): continue
            results = self.results(scope=scope, size=len(trades))
            self.console("Downloaded", results)
            yield trades

    @staticmethod
    @abstractmethod
    def parser(trades, /, **kwargs): pass


class AlpacaStockTicksDownloader(AlpacaTicksDownloader, page=AlpacaStockTicksPage, instrument=Instrument.STOCK):
    @staticmethod
    def parser(trades, /, **kwargs):
        trades["datetime"] = pd.to_datetime(trades["datetime"])
        trades = trades.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        trades = trades.rename(columns={"product": "ticker"})
        trades = trades.reset_index(drop=True, inplace=False)
        return trades


class AlpacaOptionTicksDownloader(AlpacaTicksDownloader, page=AlpacaOptionTicksPage, instrument=Instrument.OPTION):
    @staticmethod
    def parser(trades, /, **kwargs):
        trades["datetime"] = pd.to_datetime(trades["datetime"])
        trades = trades.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        trades = trades.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(trades["osi"].map(OSI).map(asdict), index=trades.index)
        trades = pd.concat([trades, contracts], axis=1)
        trades = trades.reset_index(drop=True, inplace=False)
        return trades



