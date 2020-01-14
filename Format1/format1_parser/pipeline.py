import logging
from datetime import datetime

from bs4 import BeautifulSoup

from Format1.format1_parser.format1_html import Formathtml
from Format1.format1_parser.format_1_parser import Parser
from Format5.format5_parser.formattab_parser import ParserFormat5
from config.config import CONFIG

class Format1Pipeline:

    def __init__(self):
        logging.config.fileConfig('loggerconfig.conf')
        self.logger = logging.getLogger('MainLogger')
        fh = logging.FileHandler(CONFIG["Path"]["log_path"]+'{:%Y-%m-%d}.log'.format(datetime.now()))
        formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

    def process(self, accession_no):

        text_list, registrant_name = Parser.preprocess(accession_no)
        if len(text_list)>0:
            funds_list = Parser.parse_file(accession_no,text_list)
            if len(funds_list)>0:
                header_df,table_df = Parser.post_process(accession_no,funds_list)
                if header_df.empty:
                    logging.debug("header_df is empty for accession_no:{}".format(accession_no))
                else:
                    final_df = Parser.process_df(accession_no,header_df,table_df)
                    if final_df.empty:
                        logging.debug("final_df is empty for accession_no:{}".format(accession_no))
                    else:
                        print("final_df length:{}".format(len(final_df.index)))
                        Parser.df_to_database(accession_no,final_df)
                        logging.debug("final_df created successfully for accession_no:{}".format(accession_no))
        else:
            logging.debug("No data found from the file for accssion_no:{}".format(accession_no))

    def processFormat1(self,accession_no,documentId):
        try:
            s = Parser.readFile(accession_no)
            text = BeautifulSoup(s, "lxml")
            if Formathtml.findDivCount(text) >= 1:
                s = Formathtml.putPreTag(s)

            funds_list = Parser.headerFundsList(s)
            # funds_list = Parser.headerFundsList(s)

            if (len(funds_list)) > 0:
                main_header_df, table_df = Parser.savetodataframe(funds_list)
                final_df = Parser.collapse(main_header_df, table_df)
            if final_df.empty == False:
                Parser.df_to_database(accession_no, documentId, final_df)
                Parser.df_to_excel(accession_no,final_df,documentId=0)
                self.logger.debug(accession_no)
                self.logger.debug("here")
                return 1
        except Exception as e:
            print(e)
            self.logger.debug("Exception in parsing ")
            self.logger.debug(e)


    def processFormat5(self,accession_no,doc_id):
        df1=ParserFormat5.mergeDataframe(accession_no)
        # print(df1.head())
        print("here")
        if not df1.empty:
            ParserFormat5.df_to_excel(accession_no,df1)
            return 1

