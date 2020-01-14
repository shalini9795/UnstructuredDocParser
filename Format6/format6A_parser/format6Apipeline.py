from bs4 import BeautifulSoup
import pandas as pd
from config.config import CONFIG
from utils.utility import Utils
from Format6.format6A_parser.format6Aparser import Format6parser
import os
class Format6APipeline:

    @staticmethod
    def check_table(table):
        footer_list = ['Item','Proposal','Proposed by','Vote','For/Against']
        count = 0
        for footer in footer_list:
            if str(table.text).find(footer) > -1:
                count += 1
        if count >= 3:
            return True


    def process(self,accession_no,doc_id):
        logger = Utils.add_logger()
        try:
            path = CONFIG["Path"]["file_path"] + accession_no + ".dissem"
            content = Utils.read_file(path, accession_no)
            content =Format6parser.segparsing(content)
            content = content.replace("</td>\n<tr>", "</td></tr><tr>")
            soup = BeautifulSoup(content, "html.parser")
            tables = soup.findAll('table')
            dffa=None
            prev_table = None
            finaldf = pd.DataFrame()
            for table in tables:
                try:
                    if Format6APipeline.check_table(table):
                        if prev_table is not None:
                            prev_table=prev_table + str(table.contents)
                            dffa = Format6parser.tabledetails(BeautifulSoup(prev_table,"html.parser"))
                            prev_table = None
                        else:
                            dffa = Format6parser.tabledetails(table)
                    else:
                        prev_table = str(table.contents)
                    #print('######################')
                    if dffa is not None:
                        if finaldf.empty == True:
                            finaldf = dffa
                            dffa = None
                        else:
                            #print(dffa.head())
                            finaldf = finaldf.append(dffa, sort=True)
                            dffa = None
                except Exception as error:
                    logger.error("Exception in table parsing for format6 for accession_no -{} -{}".format(accession_no,error))
            filename = os.path.join(CONFIG['Path']['output_path_format6B'], accession_no + '.xlsx')
            Utils.df_to_excel(accession_no,finaldf,filename)
            return 1
        except Exception as error:
            logger.error("Exception in format6 pipeline for accession_no -{}-{}".format(accession_no,error))
            print("Exception in format6A pipeline-{}".format(error))
            return 0

