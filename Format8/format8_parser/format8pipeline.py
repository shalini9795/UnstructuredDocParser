import pandas as pd
from Format8.format8_parser.format8parser import Format8Parser
from bs4 import BeautifulSoup
import os
from utils.utility import Utils
import logging
from config.config import CONFIG
import re
class Format8Pipeline:


    def __init__(self):
        pass

    def process(self,accession_no,doc_id):
        logger = Utils.add_logger()
        try:
            olderFundName = ''
            lstdf = []
            dfs_all = pd.DataFrame()
            path = CONFIG["Path"]["file_path"] + accession_no +".dissem"
            content = Utils.read_file(path,accession_no)
            content = Format8Parser.segparsing(content)
            # print(content)
            content = re.sub("=+","",content)
            content = content.replace("<TABLE", "@@@@@@@@@@@@@@@@@@@@@\n <TABLE")
            content = content.replace("<table", "@@@@@@@@@@@@@@@@@@@@@\n <table")
            content = content.replace("</TABLE>", "</TABLE>\n ######################")
            content = content.replace("</table>", "</table>\n ######################")
            content = content.replace("</td>\n<tr>", "</td></tr><tr>")
            content = content.replace("The Fund did not vote any proxies during this reporting period",
                                      "The Fund did not vote any proxies during this reporting period\n ######################")
            fundVoteData = content.split('######################')
            for alist in fundVoteData:
                try:
                    #print("---------------------------------------------------------------")
                    # print(alist,lstdf)
                    # print(alist)
                    df_all = Format8Parser.tableparsed(alist, olderFundName, lstdf, accession_no)
                    # print(df_all)
                    if df_all is not None:
                        dfs_batch = pd.DataFrame()
                        # col_list = df_all.columns
                        # col_list = [(lambda x: re.sub(' +',' ',x))(l) for l in col_list]
                        # df_all.columns = col_list
                        dfs_batch = Format8Parser.formDataFrame(df_all)
                        if dfs_batch.empty != True:
                            if dfs_all.empty == True:
                                # print(dfs_batch)
                                dfs_all = dfs_batch
                            else:
                                #print(dfs_batch)
                                dfs_all = dfs_all.append(dfs_batch, ignore_index=True, sort=False)
                        if "FundName" in dfs_all.columns:
                            olderFundName = dfs_all["FundName"][0]
                            lstdf = df_all.columns
                        # print(dfs_batch)
                        # print(lstdf)
                except Exception as error:
                    logger.error("Exception in for loop of Format8pipeline.process for accessionnumber:{}-{}".format(accession_no, error))
                    print("Exception in for loop of Format8pipeline.process for accessionnumber:{}-{}".format(accession_no, error))
            #print('------------Final -----------------')
            #print(dfs_all)
            #print("Column names:{}".format(dfs_all.columns))
            if dfs_all is not None:
                dfs_all = Format8Parser.remove_spaces_from_df(dfs_all)
            filename = os.path.join(CONFIG['Path']['output_path_format8'] , accession_no + '.xlsx')
            Utils.df_to_excel(accession_no,dfs_all,filename)
            return 1
        except Exception as error:
            logger.error("Exception in Format8pipeline.process for accessionnumber:{}-{}".format(accession_no, error))
            print("Exception in Format8pipeline.process for accessionnumber:{}-{}".format(accession_no,error))
            return 0