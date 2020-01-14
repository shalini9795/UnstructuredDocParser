import logging
from Format2.format2_parser.format_2_parser import Parser
import os
from config.config import CONFIG
from utils.utility import Utils


class Pipeline:

    def __init__(self):
        pass

    def process(self, accession_no,doc_id):
        logger = Utils.add_logger()
        text_list, registrant_name = Parser.preprocess(accession_no)
        if len(text_list)>0:
            funds_list = Parser.parse_file(accession_no,text_list,doc_id)
            if len(funds_list)>0:
                header_df,table_df = Parser.post_process(accession_no,funds_list)
                if header_df.empty:
                    logging.debug("header_df is empty for accession_no:{}".format(accession_no))
                else:
                    final_df = Parser.process_df(accession_no,header_df,table_df)
                    if final_df.empty:
                        logging.debug("final_df is empty for accession_no:{}".format(accession_no))
                    else:
                        logging.debug("final_df created successfully for accession_no:{}".format(accession_no))
                        print("final_df length:{}".format(len(final_df.index)))
                        final_df = final_df.drop(columns=["seq_id"])
                        Utils.df_to_database(accession_no,doc_id, final_df)
                        filename = os.path.join(CONFIG["Path"]["output_path_format2"], accession_no + ".xlsx")
                        Utils.df_to_excel(accession_no,final_df,filename)
                        return 1
        else:
            logger.debug("No data found from the file for accssion_no:{}".format(accession_no))
            return "Unsuccessfull"