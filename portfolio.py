# -*- coding: utf-8 -*-
"""
Created on Sun Jul 5 2026
@name:   Alpaca Portfolio Objects
@author: Jack Kirby Cook
@file:   alpaca/portfolio.py

"""

import pandas as pd
from types import SimpleNamespace

from alpaca.website import AlpacaDownloadURL, AlpacaDownloadPage, AlpacaDownloader
from finance.enumerations import Instrument, Position
from finance.osi import OSI
from webscraping.webdatas import WebJSON
from support.custom import ReversibleDict as RDict

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaPortfolioDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


position_mapping = RDict({Position.LONG: "buy", Position.SHORT: "sell"})
timestamp_parser = lambda string: pd.to_datetime(string)
position_parser = lambda string: position_mapping[string, True]
ticker_parser = lambda string: OSI(string).ticker
expire_parser = lambda string: OSI(string).expire
right_parser = lambda string: OSI(string).right
strike_parser = lambda string: OSI(string).strike
quantity_parser = lambda string: abs(int(string))


class AlpacaPortfolioURL(AlpacaDownloadURL, domain="https://paper-api.alpaca.markets"): pass
class AlpacaHoldingsURL(AlpacaPortfolioURL, path=["v2", "positions"], headers={"accept": "application/json"}): pass
class AlpacaAccountURL(AlpacaPortfolioURL, path=["v2", "account"], headers={"accept": "application/json"}): pass


class AlpacaHoldingsData(WebJSON, multiple=True, optional=True):
    class Asset(WebJSON.Text, key="asset", locator="asset_id", parser=str): pass
    class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
    class Expire(WebJSON.Text, key="expire", locator="symbol", parser=expire_parser): pass
    class Right(WebJSON.Text, key="right", locator="symbol", parser=right_parser): pass
    class Strike(WebJSON.Text, key="strike", locator="symbol", parser=strike_parser): pass
    class Position(WebJSON.Text, key="position", locator="side", parser=position_parser): pass
    class Quantity(WebJSON.Text, key="quantity", locator="qty", parser=quantity_parser): pass
    class Entry(WebJSON.Text, key="entry", locator="avg_entry_price", parser=float): pass

class AlpacaAccountData(WebJSON, multiple=False, optional=False):
    class Identity(WebJSON.Text, key="identity", locator="account_number", parser=str): pass
    class Date(WebJSON.Text, key="date", locator="balance_asof", parser=timestamp_parser): pass
    class Cash(WebJSON.Text, key="cash", locator="cash", parser=float): pass
    class Value(WebJSON.Text, key="value", locator="portfolio_value", parser=float): pass


class AlpacaHoldingsPage(AlpacaDownloadPage, url=AlpacaHoldingsURL, data=AlpacaHoldingsData):
    def __call__(self, *args, **kwargs):
        url = self.url(*args, **kwargs)
        json = self.load(url, *args, **kwargs)
        datas = self.data(json, *args, **kwargs)
        records = [data(*args, **kwargs) for data in datas]
        if not records: return None
        dataframe = pd.DataFrame.from_records(records)
        dataframe["expire"] = pd.to_datetime(dataframe["expire"])
        dataframe["strike"] = pd.to_numeric(dataframe["strike"])
        return dataframe

class AlpacaAccountPage(AlpacaDownloadPage, url=AlpacaAccountURL, data=AlpacaAccountData):
    def __call__(self, *args, **kwargs):
        url = self.url(*args, **kwargs)
        json = self.load(url, *args, **kwargs)
        datas = self.data(json, *args, **kwargs)
        mapping = datas(*args, **kwargs)
        series = pd.Series(mapping)
        return series


class AlpacaPortfolioDownloader(AlpacaDownloader, pages={"holdings": AlpacaHoldingsPage, "account": AlpacaAccountPage}, instrument=Instrument.OPTION):
    def __call__(self, /, **kwargs):
        account = self.pages["account"](**kwargs)
        holdings = self.pages["holdings"](**kwargs)
        if holdings is None or bool(holdings.empty): return None
        holdings = holdings.sort_values(by=["asset"], inplace=False)
        holdings = holdings.reset_index(drop=True, inplace=False)
        scope = self.scope(holdings)
        results = self.results(scope=scope, size=len(holdings.index))
        self.console("Downloaded", results)
        portfolio = SimpleNamespace(holdings=holdings, account=account)
        return portfolio




