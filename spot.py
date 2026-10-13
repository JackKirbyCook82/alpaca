# -*- coding: utf-8 -*-
"""
Created on Sun Oct 1 2026
@name:   Alpaca Spot Objects
@author: Jack Kirby Cook
@file:   alpaca/spot.py

"""

import pandas as pd
from dataclasses import asdict
from abc import ABC, abstractmethod

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
        bars = pd.DataFrame.from_records(records)
        return bars

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
    def __call__(self, products, /, price, **kwargs):
        assert price in (Price.TRADE, Price.QUOTE)
        if not isinstance(products, list): products = [products]
        quotes = list(self.downloader(products, price=price, **kwargs))
        if not quotes: return None
        quotes = pd.concat(list(quotes), axis=0)
        quotes = self.parser(quotes, **kwargs)
        return quotes

    def downloader(self, products, /, **kwargs):
        products = [products[index:index + 100] for index in range(0, len(products), 100)]
        for products in products:
            scope = self.scope(products)
            quotes = self.page(products=products, **kwargs)
            if quotes is None or bool(quotes.empty): continue
            results = self.results(scope=scope, size=len(quotes))
            self.console("Downloaded", results)
            yield quotes

    @staticmethod
    @abstractmethod
    def parser(quotes, /, **kwargs): pass


class AlpacaStockSpotDownloader(AlpacaSpotDownloader, page=AlpacaStockSpotPage, instrument=Instrument.STOCK):
    @staticmethod
    def parser(quotes, /, **kwargs):
        quotes["datetime"] = pd.to_datetime(quotes["datetime"])
        quotes = quotes.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        quotes = quotes.rename(columns={"product": "ticker"})
        quotes = quotes.reset_index(drop=True, inplace=False)
        return quotes


class AlpacaOptionSpotDownloader(AlpacaSpotDownloader, page=AlpacaOptionSpotPage, instrument=Instrument.OPTION):
    @staticmethod
    def parser(quotes, /, **kwargs):
        quotes["datetime"] = pd.to_datetime(quotes["datetime"])
        quotes = quotes.sort_values(by=["product", "datetime"], ascending=[True, False], inplace=False)
        quotes = quotes.rename(columns={"product": "osi"})
        contracts = pd.DataFrame.from_records(quotes["osi"].map(OSI).map(asdict), index=quotes.index)
        quotes = pd.concat([quotes, contracts], axis=1)
        quotes = quotes.reset_index(drop=True, inplace=False)
        return quotes

