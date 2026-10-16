# -*- coding: utf-8 -*-
"""
Created on Sat Sept 26 2026
@name:   Alpaca Tick Objects
@author: Jack Kirby Cook
@file:   alpaca/ticks.py

"""

import pandas as pd
from abc import ABC, abstractmethod

from alpaca.website import AlpacaDownloadURL, AlpacaDownloadPage, AlpacaDownloader, AlpacaParsers
from finance.enumerations import Price, Instrument
from finance.osi import OSI
from webscraping.webdatas import WebJSON

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaTradeStockTicksDownloader", "AlpacaTradeOptionTicksDownloader"]
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
        ticks = pd.DataFrame.from_records(records)
        return ticks

    def execute(self, *args, pagination=None, **kwargs):
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
    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        ticks = list(self.downloader(products, **kwargs))
        if not ticks: return None
        ticks = pd.concat(list(ticks), axis=0)
        ticks = self.parse(ticks, **kwargs)
        return ticks

    def downloader(self, products, /, **kwargs):
        products = [products[index:index + 100] for index in range(0, len(products), 100)]
        for products in products:
            scope = self.scope(products)
            ticks = self.page(products=products, **kwargs)
            if ticks is None or bool(ticks.empty): continue
            results = self.results(scope=scope, size=len(ticks))
            self.console("Downloaded", results)
            yield ticks


class AlpacaStockTicksDownloader(AlpacaTicksDownloader, page=AlpacaStockTicksPage, instrument=Instrument.STOCK): pass
class AlpacaOptionTicksDownloader(AlpacaTicksDownloader, page=AlpacaOptionTicksPage, instrument=Instrument.OPTION): pass

class AlpacaTradeStockTicksDownloader(AlpacaStockTicksDownloader, price=Price.TRADE): pass
class AlpacaTradeOptionTicksDownloader(AlpacaOptionTicksDownloader, price=Price.TRADE): pass



