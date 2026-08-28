# -*- coding: utf-8 -*-
"""
Created on Sun Jul 5 2026
@name:   Alpaca Portfolio Objects
@author: Jack Kirby Cook
@file:   alpaca/portfolio.py

"""

import pandas as pd
from datetime import date as Date
from datetime import datetime as DateTime

from finance.enumerations import Instrument, Option, Position
from finance.querys import Contract
from finance.logging import Logging
from finance.osi import OSI
from webscraping.webpages import WebStream, WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.custom import ReversibleDict as RDict
from support.files import File, Header

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaPortfolioDownloader", "AlpacaPortfolioFile"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


position_mapping = RDict({Position.LONG: "buy", Position.SHORT: "sell"})
position_parser = lambda string: position_mapping[string, True]
ticker_parser = lambda string: OSI.parse(string).ticker
expire_parser = lambda string: OSI.parse(string).expire
option_parser = lambda string: OSI.parse(string).option
strike_parser = lambda string: OSI.parse(string).strike

portfolio_typing = {"asset": str, "ticker": str, "expire": Date, "option": int, "strike": float, "position": int, "quantity": int, "entry": float}
portfolio_formatting = {"expire": lambda expire: expire.strftime("%Y%m%d"), "option": int, "position": int}
portfolio_parsing = {"expire": lambda string: DateTime.strptime(string, "%Y%m%d").date(), "option": Option, "position": Position}
portfolio_columns = ["asset", "ticker", "expire", "option", "strike", "position", "quantity", "entry"]
portfolio_header = Header(portfolio_columns, portfolio_typing, portfolio_formatting, portfolio_parsing)


class AlpacaPortfolioFile(File, header=portfolio_header):
    def results(self, orders, *args, title, **kwargs):
        pass


class AlpacaPortfolioURL(WebURL, domain="https://paper-api.alpaca.markets", path=["v2", "positions"], headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}


class AlpacaPortfolioData(WebJSON, multiple=True, optional=True):
    class AssetID(WebJSON.Text, key="asset", locator="asset_id", parser=str): pass
    class Ticker(WebJSON.Text, key="ticker", locator="symbol", parser=ticker_parser): pass
    class Expire(WebJSON.Text, key="expire", locator="symbol", parser=expire_parser): pass
    class Option(WebJSON.Text, key="option", locator="symbol", parser=option_parser): pass
    class Strike(WebJSON.Text, key="strike", locator="symbol", parser=strike_parser): pass
    class Position(WebJSON.Text, key="position", locator="side", parser=position_parser): pass
    class Quantity(WebJSON.Text, key="quantity", locator="qty", parser=int): pass
    class Entry(WebJSON.Text, key="entry", locator="avg_entry_price", parser=int): pass


class AlpacaPortfolioPage(WebJSONPage):
    def __call__(self, *args, **kwargs):
        url = AlpacaPortfolioURL(authenticator=self.authenticator)
        json = self.load(url)
        datas = AlpacaPortfolioData(json, *args, **kwargs)
        records = [data(*args, **kwargs) for data in datas]
        dataframe = pd.DataFrame.from_records(records)
        dataframe["expire"] = pd.to_datetime(dataframe["expire"])
        dataframe["strike"] = pd.to_numeric(dataframe["strike"])
        return dataframe


class AlpacaPortfolioDownloader(WebStream, Logging, page=AlpacaPortfolioPage):
    def __call__(self, **kwargs):
        portfolio = self.page(**kwargs)
        if bool(portfolio.empty): return pd.DataFrame(columns=portfolio_columns)
        scope = self.scope(portfolio, instrument=Instrument.OPTION)
        self.results(scope=scope, size=len(portfolio.index), title="Downloaded")
        key = lambda series: series.map(str) if series.name == "option" else series
        portfolio = portfolio.sort_values(by=list(Contract), inplace=False, key=key)
        portfolio = portfolio.reset_index(drop=True, inplace=False)
        return portfolio



