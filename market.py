# -*- coding: utf-8 -*-
"""
Created on Thurs Mar 19 2026
@name:   Alpaca Market Objects
@author: Jack Kirby Cook

"""

import numpy as np
import pandas as pd
from types import SimpleNamespace
from abc import ABC, abstractmethod
from datetime import datetime as Datetime
from collections import namedtuple as ntuple

from finance.concepts import Concepts, Querys, OptionOSI
from webscraping.webpages import WebJSONPage, WebStream
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaStockDownloader", "AlpacaContractDownloader", "AlpacaOptionDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


pagination_parser = lambda string: str(string) if string != "None" else None
expire_parser = lambda string: Datetime.strptime(string, "%Y-%m-%d").date()
strike_parser = lambda content: np.round(float(content), 2)


class AlpacaSecurityURL(WebURL, headers={"accept": "application/json"}):
    @staticmethod
    def headers(*args, authenticator, **kwargs):
        return {"APCA-API-KEY-ID": str(authenticator.identity), "APCA-API-SECRET-KEY": str(authenticator.code)}

class AlpacaStockURL(AlpacaSecurityURL, domain="https://data.alpaca.markets", path=["v2", "stocks"], parameters={"feed": "delayed_sip"}):
    @staticmethod
    def parameters(*args, tickers, **kwargs):
        return {"tickers": ",".join(list(tickers))}

class AlpacaOptionURL(AlpacaSecurityURL, domain="https://data.alpaca.markets", path=["v1beta1", "options"], parameters={"feed": "indicative"}):
    @staticmethod
    def parameters(*args, osis, **kwargs):
        return {"tickers": ",".join(list(osis))}


class AlpacaStockTradeURL(AlpacaStockURL, path=["trades", "latest"]): pass
class AlpacaStockQuoteURL(AlpacaStockURL, path=["quotes", "latest"]): pass
class AlpacaOptionTradeURL(AlpacaOptionURL, path=["trades", "latest"]): pass
class AlpacaOptionQuoteURL(AlpacaOptionURL, path=["quotes", "latest"]): pass


class AlpacaContractURL(AlpacaSecurityURL, domain="https://paper-api.alpaca.markets", path=["v2", "options", "contracts"], parameters={"show_deliverables": "false", "limit": "10000"}):
    @classmethod
    def parameters(cls, *args, **kwargs):
        tickers = cls.tickers(*args, **kwargs)
        expires = cls.expires(*args, **kwargs)
        pagination = cls.pagination(*args, **kwargs)
        return tickers | expires | pagination

    @staticmethod
    def tickers(*args, ticker, **kwargs):
        return {"underlying_symbols": str(ticker)}

    @staticmethod
    def expires(*args, expires=None, **kwargs):
        if expires is not None: return {"expiration_date_gte": str(expires.minimum.strftime("%Y-%m-%d")), "expiration_date_lte": str(expires.maximum.strftime("%Y-%m-%d"))}
        else: return {}

    @staticmethod
    def pagination(*args, pagination=None, **kwargs):
        if pagination is not None: return {"page_token": str(pagination)}
        else: return {}


class AlpacaContractData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=pagination_parser, multiple=False, optional=True): pass
    class Contracts(WebJSON, key="contracts", locator="//option_contracts[]", parser=Querys.Contract, multiple=True, optional=True):
        class Ticker(WebJSON.Text, key="ticker", locator="//underlying_symbol", parser=str): pass
        class Expire(WebJSON.Text, key="expire", locator="//expiration_date", parser=expire_parser): pass
        class Option(WebJSON.Text, key="option", locator="//type", parser=Concepts.Securities.Option): pass
        class Strike(WebJSON.Text, key="strike", locator="//strike_price", parser=strike_parser): pass


class AlpacaMarketPage(WebJSONPage, ABC): pass
class AlpacaSecurityPage(AlpacaMarketPage):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        Field = ntuple("Field", "name code parser")
        fields = [Field("last", "p", np.float32), Field("bid", "bp", np.float32), Field("ask", "ap", np.float32), Field("supply", "as", np.float32), Field("demand", "bs", np.float32)]
        self.__fields = fields

    def parser(self, mapping):
        return {field.name: field.parser(mapping[field.code]) for field in self.fields if field in mapping.keys()}

    @staticmethod
    def merger(trades, quotes, on):
        assert isinstance(trades, pd.DataFrame) and isinstance(quotes, pd.DataFrame)
        if trades.empty or quotes.empty: return pd.DataFrame()
        header = list(trades.columns) + [column for column in list(quotes.columns) if column not in list(trades.columns)]
        dataframe = quotes.merge(trades, how="outer", on=list(on), sort=False, suffixes=("", "_"))[header]
        return dataframe

    @abstractmethod
    def trades(self, *args, **kwargs): pass
    @abstractmethod
    def quotes(self, *args, **kwargs): pass

    @property
    def fields(self): return self.__fields


class AlpacaStockPage(AlpacaSecurityPage):
    def __call__(self, *args, tickers, **kwargs):
        assert isinstance(tickers, list)
        tickers = list(map(str, tickers))
        parameters = dict(tickers=tickers, authenticator=self.authenticator)
        trades = self.trades(**parameters)
        quotes = self.quotes(**parameters)
        stocks = self.merger(trades, quotes, on=list(Querys.Symbol))
        return stocks

    def trades(self, *args, **kwargs):
        url = AlpacaStockTradeURL(*args, **kwargs)
        json = self.load(url)["trades"]
        records = [{"ticker": ticker} | self.parser(contents) for ticker, contents in json.items()]
        dataframe = pd.DataFrame.from_records(records)
        dataframe["instrument"] = Concepts.Securities.Instrument.STOCK
        dataframe["option"] = Concepts.Securities.Option.EMPTY
        return dataframe

    def quotes(self, *args, **kwargs):
        url = AlpacaStockQuoteURL(*args, **kwargs)
        json = self.load(url)["quotes"]
        records = [{"ticker": ticker} | self.parser(contents) for ticker, contents in json.items()]
        dataframe = pd.DataFrame.from_records(records)
        dataframe["instrument"] = Concepts.Securities.Instrument.STOCK
        dataframe["option"] = Concepts.Securities.Option.EMPTY
        return dataframe


class AlpacaContractPage(AlpacaMarketPage):
    def __call__(self, *args, ticker, expires=None, strikes=None, pagination=None, **kwargs):
        parameters = dict(tickers=str(ticker), expires=expires, strikes=strikes, authenticator=self.authenticator)
        url = AlpacaContractURL(pagination=pagination, **parameters)
        json = self.source.load(url)
        datas = AlpacaContractData(json, *args, **kwargs)
        contents = [data(*args, **kwargs) for data in datas["contracts"]]
        pagination = datas["pagination"](*args, **kwargs)
        if not bool(pagination): return list(contents)
        else: return list(contents) + self(args, ticker=ticker, expire=ticker, pagination=pagination, **kwargs)


class AlpacaOptionPage(AlpacaSecurityPage):
    def __call__(self, *args, contracts, authenticator, **kwargs):
        osis = list(map(lambda contract: str(OptionOSI(contract)), contracts))
        parameters = dict(osis=osis, authenticator=self.authenticator)
        trades = self.trades(**parameters)
        quotes = self.quotes(**parameters)
        options = self.merger(trades, quotes, on=list(Querys.Contract))
        return options

    def trades(self, *args, **kwargs):
        url = AlpacaOptionTradeURL(*args, **kwargs)
        json = self.source.load(url)["trades"]
        records = [dict(OptionOSI(osi).items()) | self.parser(contents) for osi, contents in json.items()]
        dataframe = pd.DataFrame.from_records(records)
        dataframe["instrument"] = Concepts.Securities.Instrument.OPTION
        return dataframe

    def quotes(self, *args, **kwargs):
        url = AlpacaOptionQuoteURL(*args, **kwargs)
        json = self.source.load(url)["quotes"]
        records = [dict(OptionOSI(osi).items()) | self.parser(contents) for osi, contents in json.items()]
        dataframe = pd.DataFrame.from_records(records)
        dataframe["instrument"] = Concepts.Securities.Instrument.OPTION
        return dataframe


class AlpacaMarketDownloader(WebStream, ABC):
    @abstractmethod
    def downloader(self, *args, **kwargs): pass


class AlpacaSecurityDownloader(AlpacaMarketDownloader, ABC):
    def __init__(self, *args, capacity=100, **kwargs):
        super().__init__(*args, **kwargs)
        assert isinstance(capacity, int)
        self.__capacity = capacity

    @property
    def capacity(self): return self.__capacity


class AlpacaStockDownloader(AlpacaSecurityDownloader, page=AlpacaContractPage):
    def __call__(self, *args, symbols, **kwargs):
        assert isinstance(symbols, list)
        tickers = list({symbol.ticker for symbol in symbols})
        stocks = self.downloader(*args, tickers=tickers, **kwargs)
        stocks = pd.concat(list(stocks), axis=1)
#        self.console(title="Downloaded")
        return stocks

    def downloader(self, *args, tickers, **kwargs):
        tickers = [tickers[index:index+self.capacity] for index in range(0, len(tickers), self.capacity)]
        for tickers in tickers:
            stocks = self.page(tickers=tickers)
            yield stocks


class AlpacaContractDownloader(AlpacaMarketDownloader, page=AlpacaContractPage):
    def __call__(self, *args, symbols, expires=None, strikes=None, **kwargs):
        assert isinstance(symbols, list)
        tickers = list({symbol.ticker for symbol in symbols})
        contracts = self.downloader(*args, tickers=tickers, expires=expires, strikes=strikes, **kwargs)
        contracts = list(contracts)
        contracts.sort(key=lambda contract: (contract.ticker, contract.expire))
#        self.console(title="Downloaded")
        return contracts

    def downloader(self, *args, tickers, expires=None, strikes=None, **kwargs):
        for ticker in tickers:
            parameters = dict(ticker=ticker, expires=expires, strikes=strikes)
            contracts = self.page(**parameters)
            for contract in contracts:
                yield contract


class AlpacaOptionDownloader(AlpacaSecurityDownloader, page=AlpacaContractPage):
    def __call__(self, *args, contracts, **kwargs):
        assert isinstance(contracts, list)
        options = self.downloader(*args, contracts=contracts, **kwargs)
        options = pd.concat(list(options), axis=1)
        tickers = list({contract.ticker for contract in contracts})
        tickers = "|".join(tickers)
        expires = list({contract.expire for contract in contracts})
        expires = SimpleNamespace(minimum=min(expires), maximum=max(expires))
        expires = f"{expires.min.strftime('%Y%m%d')}->{expires.max.strftime('%Y%m%d')}"
#        self.console(title="Downloaded")
        return options

    def downloader(self, *args, contracts, **kwargs):
        contracts = [contracts[index:index+self.capacity] for index in range(0, len(contracts), self.capacity)]
        for contracts in contracts:
            options = self.page(contracts=contracts)
            yield options


