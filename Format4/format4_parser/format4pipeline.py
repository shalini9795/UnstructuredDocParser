from datetime import datetime
import pandas as pd
from Format4.format4_parser.format4parser import Format4Parser
from bs4 import BeautifulSoup
import os
import logging
from config.config import CONFIG
from utils.utility import Utils


class Format4Pipeline:


    def __init__(self):
        pass


    def process(self,accession_no,doc_id):
        logger = Utils.add_logger()
        try:
            path = CONFIG["Path"]["file_path"] + accession_no +".dissem"
            content = Utils.read_file(path,accession_no)
            content = content.replace('&#160;', '\xa0')
            soup = BeautifulSoup(content, "html.parser")
            contents = soup.text
            contents = contents.replace('SIGNATURES', '----------------------------------')
            contents = contents.replace('FUND:', '############### \n FUND:')
            fundlist = contents.split('###############')
        except Exception as error:
            logger.error("Exception in parsing through bs4 for accession_no -{} -{}".format(accession_no,error))
            fundlist = []
            return "unsuccessfull"
        i = 0
        final_df = pd.DataFrame()
        # print(fundlist[1])
        for list in fundlist:
            if i > 0:
                sr = list.split('ISSUER')
                fName = Format4Parser.fundName(sr[0])
                #print(fName)
                df = Format4Parser.companyfundDetails(list, fName)
                try:
                    final_df = final_df.append(df)
                except Exception as error:
                    logger.error("Exception in pipeline for df merging for accession_no -{} -{}".format(accession_no,error))
                    return "unsuccessfull"
            i = 1
        final_df = Format4Parser.concat_rowData(final_df)
        final_df = final_df.drop(columns=['ID'])
        final_df.rename(columns={"ISSUER":"CompanyName","MEETING DATE":"MeetingDate","Proposal No":"ProposalNumber",
                                 "PROPOSAL":"Proposal","PROPOSED BY":"ProposedBy","VOTED?":"Voted",
                                 "MGMT":"ForAgainstManagement","TICKER":"Ticker","VOTE CAST":"VoteCast"},inplace=True)
        final_df["MeetingDate"] = final_df["MeetingDate"].apply(Utils.parsing_date)
        final_df["AccesssionNumber"] = accession_no
        final_df["DocumentId"] = doc_id
        Utils.df_to_database(accession_no,doc_id,final_df)
        filename = os.path.join(CONFIG["Path"]["output_path_format4"], accession_no + ".xlsx")
        Utils.df_to_excel(accession_no,final_df,filename)
        return 1