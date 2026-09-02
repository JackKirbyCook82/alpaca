# -*- coding: utf-8 -*-
"""
Created on Sun Jul 5 2026
@name:   Alpaca Portfolio Objects
@author: Jack Kirby Cook
@file:   alpaca/portfolio.py

"""

import pandas as pd
from types import SimpleNamespace

from finance.enumerations import Instrument, Position
from finance.logging import Logging
from finance.osi import OSI
from webscraping.webpages import WebStream, WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.custom import ReversibleDict as RDict

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaPortfolioDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


position_mapping = RDict({Position.LONG: "buy", Position.SHORT: "sell"})
timestamp_parser = lambda string: pd.to_datetime(string)
position_parser = lambda string: position_mapping[string, True]
ticker_parser = lambda string: OSI.parse(string).ticker
expire_parser = lambda string: OSI.parse(string).expire
option_parser = lambda string: OSI.parse(string).option
strike_parser = lambda string: OSI.parse(string).strike
portfolio_columns = ["asset", "ticker", "expire", "option", "strike", "position", "quantity", "entry"]


class AlpacaPortfolioURL(WebURL, domain="https://paper-api.alpaca.markets"):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}

class AlpacaHoldingsURL(WebURL, path=["v2", "account"], headers={"accept": "application/json"}): pass
class AlpacaAccountURL(WebURL, path=["v2", "positions"], headers={"accept": "application/json"}): pass


class AlpacaHoldingsData(WebJSON, multiple=True, optional=True):
    class Asset(WebJSON.Text, key="asset", locator="asset_id", parser=str): pass
    class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
    class Expire(WebJSON.Text, key="expire", locator="symbol", parser=expire_parser): pass
    class Option(WebJSON.Text, key="option", locator="symbol", parser=option_parser): pass
    class Strike(WebJSON.Text, key="strike", locator="symbol", parser=strike_parser): pass
    class Position(WebJSON.Text, key="position", locator="side", parser=position_parser): pass
    class Quantity(WebJSON.Text, key="quantity", locator="qty", parser=int): pass
    class Entry(WebJSON.Text, key="entry", locator="avg_entry_price", parser=float): pass

class AlpacaAccountData(WebJSON, multiple=False, optional=False):
    class Identity(WebJSON.Text, key="identity", locator="account_number", parser=str): pass
    class Date(WebJSON.Text, key="date", locator="balance_asof", parser=timestamp_parser): pass
    class Cash(WebJSON.Text, key="cash", locator="cash", parser=float): pass
    class Value(WebJSON.Text, key="value", locator="portfolio_value", parser=float): pass


class AlpacaHoldingsPage(WebJSONPage):
    def __call__(self, *args, **kwargs):
        url = AlpacaPortfolioURL(authenticator=self.authenticator)
        json = self.load(url)
        datas = AlpacaHoldingsData(json, *args, **kwargs)
        records = [data(*args, **kwargs) for data in datas]
        dataframe = pd.DataFrame.from_records(records)
        dataframe["expire"] = pd.to_datetime(dataframe["expire"])
        dataframe["strike"] = pd.to_numeric(dataframe["strike"])
        return dataframe

class AlpacaAccountPage(WebJSONPage):
    def __call__(self, *args, **kwargs):
        url = AlpacaAccountURL(authenticator=self.authenticator)
        json = self.load(url)
        datas = AlpacaAccountData(json, *args, **kwargs)
        mapping = datas(*args, **kwargs)
        series = pd.Series(mapping)
        return series


class AlpacaPortfolioDownloader(WebStream, Logging, pages={"holdings": AlpacaHoldingsPage, "account": AlpacaAccountPage}):
    def __call__(self, /, **kwargs):
        holdings = self.page["holdings"](**kwargs)
        if bool(holdings.empty): holdings = pd.DataFrame(columns=portfolio_columns)
        holdings = holdings.sort_values(by=["asset"], inplace=False)
        holdings = holdings.reset_index(drop=True, inplace=False)
        account = self.page["account"](**kwargs)
        scope = self.scope(holdings, instrument=Instrument.OPTION)
        self.results(scope=scope, size=len(holdings.index), title="Downloaded")
        portfolio = SimpleNamespace(holdings=holdings, account=account)
        return portfolio



