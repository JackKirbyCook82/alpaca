# -*- coding: utf-8 -*-
"""
Created on Mon Jan 13 2025
@name:   Aplaca Market Objects
@author: Jack Kirby Cook

"""

import logging
import pandas as pd
from webscraping.webpages import WebJSONPage
from webscraping.webdatas import WebJSON
from webscraping.weburl import WebURL
from support.mixins import Emptying, Sizing, Partition

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = []
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"
__logger__ = logging.getLogger(__name__)


class AlpacaURL(WebURL, domain="https://data.alpaca.markets"): pass
class AlpacaStockURL(AlpacaURL, path=["v2", "stocks"], parms={"currency": "USD", "feed": "delayed_sip"}):
    @staticmethod
    def parms(*args, tickers, **kwargs):
        assert isinstance(tickers, list)
        return {"symbol": ",".join(list(tickers)) + ".json"}

class AlpacaOptionURL(AlpacaURL, path=["v1beta1", "options"], parms={"feed": "indicative"}):
    @staticmethod
    def parms(*args, contracts, **kwargs):
        assert isinstance(contracts, list)
        return {"symbol": ",".join(list(contracts)) + ".json"}


class AlpacaStockTradeURL(AlpacaStockURL, path=["trades", "latest"]): pass
class AlpacaStockQuoteURL(AlpacaStockURL, path=["quotes", "latest"]): pass
class AlpacaOptionTradeURL(AlpacaOptionURL, path=["trades", "latest"]): pass
class AlpacaOptionQuoteURL(AlpacaOptionURL, path=["quotes", "latest"]): pass

class AlpacaContractURL(AlpacaURL, path=["v1beta1", "options", "snapshots"], parms={"feed": "indicative", "limit": 1000}):
    @staticmethod
    def path(*args, ticker, **kwargs): return [str(ticker).upper()]
    @classmethod
    def parms(cls, *args, **kwargs):
        expires = cls.expires(*args, **kwargs)
        strikes = cls.strikes(*args, **kwargs)
        pagination = cls.pagination(*args, **kwargs)
        option = cls.option(*args, **kwargs)
        return expires | strikes | pagination | option

    @staticmethod
    def expires(*args, expires, **kwargs): return {"strike_price_gte": str(expires.minimum.strftime("%Y-%m-%d")), "strike_price_lte": str(expires.maximum.strftime("%Y-%m-%d"))} if bool(expires) else {}
    @staticmethod
    def strikes(*args, strikes, **kwargs): return {"expiration_date_gte": f"{strikes.minimum}:.02f", "expiration_date_lte": f"{strikes.maximum}:.02f"} if bool(strikes) else {}
    @staticmethod
    def pagination(*args, pagination=None, **kwargs): return {"page_token": str(pagination)} if bool(pagination) else {}


class AlpacaData(WebJSON.Mapping, multiple=False, optional=False, parsers=PARSERS): pass
class AlpacaStockTradeData(AlpacaData, locator="//trades"): pass
class AlpacaStockQuoteData(AlpacaData, locator="//quotes"): pass
class AlpacaOptionTradeData(AlpacaData, locator="//trades"): pass
class AlpacaOptionQuoteData(AlpacaData, locator="//quotes"): pass

class AlpacaContractData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, locator="next_page_token", key="pagination", parser=PARSER): pass
    class Contracts(WebJSON.Mapping, locator="snapshots", key="contracts"):
        def execute(self, *args, **kwargs):
            contents = super().execute(*args, **kwargs)
            assert isinstance(contents, dict)
            contracts = list(contents.keys())
            contracts = list(map(Contract, contracts))
            return contracts


class AlpacaStockTradePage(WebJSONPage, url=AlpacaStockTradeData, data={(STOCK, TRADE), AlpacaStockTradeData}): pass
class AlpacaStockQuotePage(WebJSONPage, url=AlpacaStockQuoteData, data={(STOCK, QUOTE), AlpacaStockQuoteData}): pass
class AlpacaOptionTradePage(WebJSONPage, url=AlpacaOptionTradeData, data={(OPTION, TRADE): AlpacaOptionTradeData}): pass
class AlpacaOptionQuotePage(WebJSONPage, url=AlpacaOptionQuoteData, data={(OPTION, QUOTE): AlpacaOptionQuoteData}): pass

class AlpacaContractPage(WebJSONPage, url=AlpacaContractURL, data=AlpacaContractData):
    def execute(self, *args, **kwargs):
        url = self.url(*args, **kwargs)
        self.load(url)
        contracts = self.data["contract"](self.content, *args, **kwargs)
        pagination = self.data["pagination"](self.content, *args, **kwargs)
        if not bool(pagination): return list(contracts)
        else: return list(contracts) + self.execute(*args, pagination=pagination, **kwargs)


class AlpacaContractDownloader(object):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page = AlpacaContractPage(*args, **kwargs)

    def execute(self, *args, symbols, expires, **kwargs):
        assert isinstance(symbols, list)
        if not bool(symbols): return
        for symbol in iter(symbols):
            parameters = dict(ticker=symbol.ticker, expires=expires)
            contracts = self.page(*args, **parameters, **kwargs)
            assert isinstance(contracts, list)
            string = f"Downloaded: {repr(self)}|{str(CONTRACT)}|{str(symbol)}[{len(contracts):.0f}]"
            __logger__.info(string)
            if not bool(contracts): continue
            yield {(CONTRACT,): contracts}


class AlpacaStockDownloader(Sizing, Emptying, Partition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        trade = AlpacaStockTradePage(*args, **kwargs)
        quote = AlpacaStockQuotePage(*args, **kwargs)
        self.pages = dict(trade=trade, quote=quote)

    def execute(self, *args, symbols, **kwargs):
        assert isinstance(symbols, list)
        if not bool(symbols): return
        parameters = dict(tickers=[str(symbol.ticker).upper() for symbol in symbols])
        stocks = {dataset: page(*args, **parameters, **kwargs) for dataset, page in self.pages.items()}
        stocks = pd.concat(list(stocks.values()), axis=1)
        for partition, dataframe in self.partition(stocks, by=Symbol):
            contents = {(STOCK, dataset): dataframe[columns] for dataset, columns in {TRADE: TRADE_COLUMNS, QUOTE: QUOTE_COLUMNS}.items()}
            for dataset, content in contents.items():
                string = "|".join(list(map(str, dataset)))
                size = self.size(content)
                string = f"Downloaded: {repr(self)}|{str(string)}|{str(partition)}[{int(size):.0f}]"
                __logger__.info(string)
            if self.empty(contents): continue
            yield contents


class AlpacaOptionDownloader(Sizing, Emptying, Partition):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        trade = AlpacaOptionTradePage(*args, **kwargs)
        quote = AlpacaOptionQuotePage(*args, **kwargs)
        self.pages = dict(trade=trade, quote=quote)

    def execute(self, *args, contracts, **kwargs):
        assert isinstance(contracts, list)
        if not bool(contracts): return
        parameters = dict(contract=[str(contract.toOSI()) for contract in contracts])
        options = {dataset: page(*args, **parameters, **kwargs) for dataset, page in self.pages.items()}
        options = pd.concat(list(options.values()), axis=1)
        for partition, dataframe in self.partition(options, by=Settlement):
            contents = {(OPTION, dataset): dataframe[columns] for dataset, columns in {TRADE: COLUMNS, QUOTE: COLUMNS}.items()}
            for dataset, content in contents.items():
                string = "|".join(list(map(str, dataset)))
                size = self.size(dataframe)
                string = f"Downloaded: {repr(self)}|{str(string)}|{str(partition)}[{int(size):.0f}]"
                __logger__.info(string)
            if self.empty(contents): return
            return contents





