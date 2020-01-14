from bs4 import BeautifulSoup as bs4, BeautifulSoup
import glob
import os
import re
import contextlib
import pandas as pd
import lxml.html.clean as clean

class Format3html:

    @staticmethod
    def cleanMe(html):
        s = clean.defs.safe_attrs
        cleaner = clean.Cleaner(safe_attrs_only=True, safe_attrs=frozenset())
        cleansed = cleaner.clean_html(html)
        # cleansed=cleansed.strip("&nbsp;")
        # cleansed=cleansed.replace("&nbsp;","")
        cleansed=cleansed.replace("\xa0"," ")
        cleansed.strip("<b>").strip('<u>').strip('</b>').strip('</u')
        # cleansed.strip("<P>").strip("</P>")
        cleansed=cleansed.replace("\n"," ")
        # cleansed.replace("\n   "," ")
        cleansed= re.sub(r'\\n\s*','',cleansed)
        # print("clean",cleansed)

        # cleansed.replace("For/Agnst\n    Mgmt","For/Agnst Mgmt")
        # cleansed.replace("Vote\n    Cast","Vote Cast")
        # cleansed = re.sub(r'Vote(\n)+\s+Cast', '</html>', cleansed)
        return cleansed

# if __name__=="__main__":
#     c=Format3html()
#     c.cleanMe("Vote\n    Cast")
