from datetime import datetime
import logging
import logging.config

from bs4 import BeautifulSoup

from config.config import CONFIG
from Format5.format5_parser.formattab_parser import ParserFormat5

class Format5Pipeline:

    def __init__(self):
        logging.config.fileConfig('loggerconfig.conf')
        self.logger = logging.getLogger('MainLogger')
        fh = logging.FileHandler(CONFIG["Path"]["log_path"]+'{:%Y-%m-%d}.log'.format(datetime.now()))
        formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def processFormat5(self,accession_no,documentId,filingLogId):
        try:
            df1 = ParserFormat5.mergeDataframe(accession_no)
            # print(df1.head())
            print("here")
            if not df1.empty:
                #ParserFormat5.df_to_excel(accession_no, df1)
                ParserFormat5.df_to_sql(accession_no, documentId, df1)
                return 1
        except Exception as e:
            self.logger.debug("Exception in parsing for format1")
            self.logger.debug(accession_no)
            self.logger.error(e)

