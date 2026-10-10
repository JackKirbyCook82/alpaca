# -*- coding: utf-8 -*-
"""
Created on Sat Sept 26 2026
@name:   Alpaca Contract Objects
@author: Jack Kirby Cook
@file:   alpaca/contracts.py

"""

from alpaca.website import AlpacaDownloadURL, AlpacaDownloadPage, AlpacaDownloader, AlpacaParsers
from finance.enumerations import Instrument, Option
from finance.querys import Contract
from webscraping.webdatas import WebJSON

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaContractDownloader"]
__copyright__ = "Copyright 2026, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaContractURL(AlpacaDownloadURL, domain="https://paper-api.alpaca.markets", path=["v2", "options", "contracts"], parameters={"show_deliverables": "false", "limit": "10000"}, headers={"accept": "application/json"}):
    def parameters(self, *args, **kwargs):
        products = self.products(*args, **kwargs)
        expires = self.expires(*args, **kwargs)
        strikes = self.strikes(*args, **kwargs)
        pagination = self.pagination(*args, **kwargs)
        return products | expires | strikes | pagination

    @staticmethod
    def products(*args, product, **kwargs): return {"underlying_symbols": str(product)}


class AlpacaContractData(WebJSON, multiple=False, optional=False):
    class Pagination(WebJSON.Text, key="pagination", locator="//next_page_token", parser=AlpacaParsers.pagination, optional=True): pass
    class Contracts(WebJSON, key="contracts", locator="//option_contracts[]", parser=Contract, multiple=True, optional=True):
        class Ticker(WebJSON.Text, key="ticker", locator="//underlying_symbol", parser=str): pass
        class Expire(WebJSON.Text, key="expire", locator="//expiration_date", parser=AlpacaParsers.expire): pass
        class Option(WebJSON.Text, key="option", locator="//type", parser=Option): pass
        class Strike(WebJSON.Text, key="strike", locator="//strike_price", parser=AlpacaParsers.strike): pass


class AlpacaContractPage(AlpacaDownloadPage, url=AlpacaContractURL, data=AlpacaContractData):
    def __call__(self, *args, product, expires, strikes, **kwargs):
        assert expires is not None and bool(expires)
        assert strikes is not None and bool(strikes)
        parameters = dict(product=product, expires=expires, strikes=strikes)
        contracts = self.execute(**parameters)
        return contracts

    def execute(self, *args, pagination=None, **kwargs):
        url = self.url(*args, pagination=pagination, **kwargs)
        json = self.load(url, *args, **kwargs)
        datas = self.data(json, *args, **kwargs)
        records = [data(*args, **kwargs) for data in datas["contracts"]]
        pagination = datas["pagination"](*args, **kwargs)
        if not bool(pagination): return list(records)
        else: return list(records) + self.execute(*args, pagination=pagination, **kwargs)


class AlpacaContractDownloader(AlpacaDownloader, page=AlpacaContractPage, instrument=Instrument.STOCK):
    def __call__(self, products, /, **kwargs):
        if not isinstance(products, list): products = [products]
        contracts = self.downloader(products, **kwargs)
        contracts = list(contracts)
        contracts.sort(key=lambda contract: (contract.ticker, contract.expire))
        return contracts

    def downloader(self, products, /, **kwargs):
        for product in products:
            scope = self.scope([product])
            contracts = self.page(product=product, **kwargs)
            results = self.results(scope=scope, size=len(contracts))
            self.console("Downloaded", results)
            for contract in contracts: yield contract



