import logging
from datetime import datetime
from config.config import CONFIG
from bs4 import BeautifulSoup

from Format3.format_3A.format3a_parser import Format3AParser


class Format3APipeline:

    def __init__(self):
        logging.config.fileConfig('loggerconfig.conf')
        self.logger = logging.getLogger('MainLogger')
        fh = logging.FileHandler(CONFIG["Path"]["log_path"]+'{:%Y-%m-%d}.log'.format(datetime.now()))
        formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def processFormat3A(self,accession_no,documentId):
        try:
            contents=Format3AParser.read_file(accession_no)
            sreqData=Format3AParser.segment_selector(contents)
            listreqData = Format3AParser.data_cleaner(sreqData)
            df_final = Format3AParser.data_extractor(listreqData)
            Format3AParser.df_toexcel(df_final,accession_no,documentId)
        except Exception as e:
            print("Exception in parsing for accession no{} is {}: ".format(accession_no,e))

