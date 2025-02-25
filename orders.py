# -*- coding: utf-8 -*-
"""
Created on Tues Feb 18 2025
@name:   Aplaca Order Objects
@author: Jack Kirby Cook

"""

from support.mixins import Logging

__version__ = "1.0.0"
__author__ = "Jack Kirby Cook"
__all__ = ["AlpacaOrderUploader"]
__copyright__ = "Copyright 2023, Jack Kirby Cook"
__license__ = "MIT License"


class AlpacaURL():
    pass

class AlpacaPortfolioURL():
    pass

class AlpacaOrderURL():
    pass


class AlpacaPayload():
    pass

class AlpacaOrderPayload():
    pass


class AlpacaPortfolioPage():
    pass

class AlpacaOrderPage():
    pass


class AlpacaOrderUploader(Logging, title="Uploaded"):
    def execute(self, orders, *args, **kwargs):
        pass




