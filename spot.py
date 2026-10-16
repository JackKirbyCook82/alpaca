# -*- coding: utf-8 -*-
"""
Created on Sun Oct 1 2026
@name:   Alpaca Spot Objects
@author: Jack Kirby Cook
@file:   alpaca/spot.py

"""

import pandas as pd
from abc import ABC

from alpaca.website import AlpacaDownloadURL, AlpacaDownloadPage, AlpacaDownloader
from finance.enumerations import Price, Instrument
from finance.osi import OSI

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockSpotDownloader", "AlpacaOptionSpotDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaSpotURL(AlpacaDownloadURL, ABC, domain="https://data.alpaca.markets"):
    @staticmethod
    def path(*args, price, **kwargs): return [str(price).lower() + "s", "latest"]

class AlpacaStockSpotURL(AlpacaSpotURL, path=["v2", "stocks"], parameters={"feed": "sip"}):
    @staticmethod
    def parameters(*args, products, **kwargs): return {"symbols": ",".join(list([symbol.ticker for symbol in products]))}

class AlpacaOptionSpotURL(AlpacaSpotURL, path=["v1beta1", "options"], parameters={"feed": "indicative"}):
    @staticmethod
    def parameters(*args, products, **kwargs): return {"symbols": ",".join([str(OSI(product)) for product in products])}


class AlpacaSpotPage(AlpacaDownloadPage, ABC):
    def __call__(self, *args, **kwargs):
        records = self.execute(*args, **kwargs)
        if not records: return None
        spot = pd.DataFrame.from_records(records)
        return spot

    def execute(self, *args, price, **kwargs):
        assert price in (Price.TRADE, Price.QUOTE)
        key = str(price).lower() + "s"
        url = self.url(*args, price=price, **kwargs)
        json = self.load(url, *args, **kwargs)
        records = self.records(json[key], *args, **kwargs)
        return list(records)


class AlpacaStockSpotPage(AlpacaSpotPage, url=AlpacaStockSpotURL, instrument=Instrument.STOCK): pass
class AlpacaOptionSpotPage(AlpacaSpotPage, url=AlpacaOptionSpotURL, instrument=Instrument.OPTION): pass


class AlpacaSpotDownloader(AlpacaDownloader):
    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        spot = list(self.downloader(products, **kwargs))
        if not spot: return None
        spot = pd.concat(list(spot), axis=0)
        spot = self.parse(spot, **kwargs)
        return spot

    def downloader(self, products, /, **kwargs):
        products = [products[index:index + 100] for index in range(0, len(products), 100)]
        for products in products:
            scope = self.scope(products)
            quotes = self.page(products=products, price=Price.TRADE, **kwargs)
            trades = self.page(products=products, price=Price.QUOTE, **kwargs)
            if quotes is None and trades is None: continue
            if bool(quotes.empty) and bool(trades.empty): continue
            if quotes is None or bool(quotes.empty): yield trades
            if trades is None or bool(trades.empty): yield quotes
            spot = pd.merge(quotes, trades, on=['product', 'datetime'], how='outer')
            spot = spot.sort_values(['product', 'datetime']).reset_index(drop=True)
            results = self.results(scope=scope, size=len(spot))
            self.console("Downloaded", results)
            yield quotes


class AlpacaStockSpotDownloader(AlpacaSpotDownloader, page=AlpacaStockSpotPage, instrument=Instrument.STOCK): pass
class AlpacaOptionSpotDownloader(AlpacaSpotDownloader, page=AlpacaOptionSpotPage, instrument=Instrument.OPTION): psas



