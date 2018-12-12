u"""
Created on 12/12/18
by fccoelho
license: GPL V3 or Later
"""
import os
from ftplib import FTP
from pysus.utilities.readdbc import read_dbc
from dbfread import DBF
import pandas as pd


def profissionais(state: str, year: int, month: int = 0):
    """
    Fetch registry of health professionals
    :param state:
    :param year:
    :param month:
    :return:
    """
    state = state.upper()
    month = str(month).zfill(2)
    ftp = FTP('ftp.datasus.gov.br')
    ftp.login()
