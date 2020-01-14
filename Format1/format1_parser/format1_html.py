from bs4 import BeautifulSoup as bs4, BeautifulSoup
import glob
import os
import re
import contextlib
import pandas as pd
import lxml.html.clean as clean

class Formathtml:

    @staticmethod
    def findDivCount(soup):
        return len(soup.find_all('div'))

    @staticmethod
    def cleanMe(html):
        s = clean.defs.safe_attrs
        cleaner = clean.Cleaner(safe_attrs_only=True, safe_attrs=frozenset())
        cleansed = cleaner.clean_html(html)
        return cleansed

    @staticmethod
    def putPreTag(cleantext):
        try:
            substr = re.compile("(\*+)((.*)+)(\*+)")
            inserttxt = "<pre>"
            idx = substr.search(cleantext).span()
            cleantext = cleantext[:idx[1]] + inserttxt + cleantext[idx[1]:]
        except Exception as e:
            print("exception in putting pre tag", e)
        return cleantext




    # content = read_file('NPX_Format2_parsing/format2_clean.html')
    # fileFormat12 = BeautifulSoup(content, "lxml")
    # format12 = fileFormat12.text
    # fundVoteData = str(fileFormat12).split('<table>')
    # fileFormat12.find_all(is_table_after_subclass)