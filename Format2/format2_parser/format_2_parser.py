from bs4 import BeautifulSoup as bs
import re
import pandas as pd
from config.config import CONFIG
import logging
import numpy as np
from datetime import datetime
import os
from utils.utility import Utils
case3 = "* Management position unknown"
case4 = "ANNUAL REPORT OF PROXY VOTING RECORD OF REGISTERED MANAGEMENT INVESTMENT COMPANY"

class Parser():


    @staticmethod
    def parsing_date(text):
        text = text.lstrip(' ').rstrip(' ')
        # print("in date",text)
        for fmt in ('%d-%b-%Y', '%B %d, %Y', '%Y-%m-%d', '%d.%m.%Y', '%d %B, %Y', '%B %d,%Y'):
            try:
                return datetime.strptime(text, fmt)
            except Exception as e:
                print("Exception in date conversion", e)
                return text

    @staticmethod
    def replace_empty(text):
        if len(text)==0:
            return None
        else:
            return  text


    @staticmethod
    def preprocess(accession_no):
        logger = Utils.add_logger()
        try:
            path = os.path.join(CONFIG["Path"]["file_path"],accession_no + ".dissem")
            file = open(path,'r',encoding='utf-8')
            s = file.read()
            raw = bs(s,"lxml")
            val = raw.get_text()
            result = raw.find_all(text = re.compile("NAME OF REGISTRANT"))
            for val in result:
                v = val.split('\n')
            for i, str in enumerate(v):
                if "NAME OF REGISTRANT" in str:
                    temp = v[i].split(':')
            Registrant_Name = temp[1].strip()
            logging.debug("Registrant Name for file:{} is {}".format(accession_no,Registrant_Name))
            res = raw.find_all(text=re.compile(Registrant_Name))
            res = raw.getText()
            text = ""
            for value in res:
                text += value
            result_list = re.split(r"-{20,}",text)
        except Exception as error:
            logger.error("Error in preprocess for file-{}-{}".format(accession_no,error))
            return list(),""

        return result_list, Registrant_Name



    @staticmethod
    def parse_file(accession_no,text_list,doc_id):
        logger = Utils.add_logger()
        try:
            funds_list = list()
            seq_id = 1  # sequencing
            prev_fundname = ""
            for index, temp in enumerate(text_list):
                split = re.split("\n", temp)
                for x, spli in enumerate(split):
                    num = x + 3
                    table_list = list()
                    if "Proposal Vote" in spli:
                        #print("Table Data")
                        row_list = list()
                        table_list.append(seq_id - 1)
                        while (num < (len(split)-2)) and (case3 not in split[num]) and (case4 not in split[num]):
                            row_text = split[num]
                            # print(row_text)
                            if len(row_text) > 0:
                                temp_text = ""
                                temp_text += row_text[:6].strip()
                                temp_text += "\t" + row_text[6:65].strip()
                                temp_text += "\t" + row_text[65:79].strip()
                                temp_text += "\t" + row_text[79:110].strip()
                                temp_text += "\t" + row_text[110:].strip()
                                table_list.append(temp_text)
                                #print(temp_text)
                            num += 1
                        funds_list.append(table_list)
                    sep = re.split("\s{3,}", spli)
                    # print(sep)
                    header_list = list()
                    for i, s in enumerate(sep):
                        # print(s)
                        if "Agenda Number:" in s:
                            fund = text_list[index - 1]
                            # print(fund)
                            fund_name = re.split(r'\s{2,}', fund)
                            fund_name = fund_name[-1]
                            fund_name = re.sub("\n", "", fund_name)
                            fund_name = fund_name.strip()
                            if len(fund_name) > 1:
                                prev_fundname = fund_name
                            else:
                                fund_name = prev_fundname
                            #print("fund_name:{}".format(fund_name))
                            t = s.split(':')
                            agenda_number = t[1].strip()
                            inc_name = sep[i - 1]
                            inc_name = inc_name.strip()
                            #print("Company Name:{} \nAgenda_number:{}".format(inc_name, agenda_number))
                        elif "Security:" in s:
                            t = s.split(':')
                            security = t[1].strip()
                            #print("Security:", security)
                        elif "Meeting Type:" in s:
                            t = s.split(':')
                            meeting_type = t[1].strip()
                            #print("Meeting_type:", meeting_type)
                        elif "Meeting Date:" in s:
                            t = s.split(':')
                            meeting_date = t[1].strip()
                            #print("Meeting_date:", meeting_date)
                        elif "Ticker:" in s:
                            t = s.split(':')
                            ticker = t[1].strip()
                            #print("Ticker:", ticker)
                        elif "ISIN:" in s:
                            t = s.split(':')
                            ISIN = t[1].strip()
                            #print("ISIN:", ISIN)
                            #print("")
                            header_list.append(seq_id)
                            header_list.append(doc_id)
                            header_list.append(accession_no)
                            header_list.append(fund_name)
                            header_list.append(inc_name)
                            header_list.append(security)
                            header_list.append(meeting_date)
                            header_list.append(meeting_type)
                            header_list.append(ISIN)
                            header_list.append(ticker)
                            header_list.append(agenda_number)
                            funds_list.append(header_list)
                            seq_id += 1
        except Exception as error:
            logger.error("Error in parsing the file for accession_no-{}-{}".format(accession_no,error))
            print("Error in parsing file for accession_no-{}-{}".format(accession_no,error))
            return list()

        return funds_list


    @staticmethod
    def post_process(accession_no,funds_list):
        logger = Utils.add_logger()
        try:
            seq_id = 1
            main_header_df = pd.DataFrame(
                columns=["seq_id","DocumentId","AccesssionNumber","FundName","CompanyName","SecurityId","MeetingDate","MeetingType",
                                       "ISIN","Ticker","AgendaNumber"])
            table_df = pd.DataFrame(
                columns=["seq_id","ProposalNumber","Proposal","ProposedBy","ForAgainstManagement","VoteCast"])
            for i, data in enumerate(funds_list):
                if i % 2 == 0:
                    #print("<-----Header Data----->")
                    temp = np.asarray(data)
                    temp = temp.reshape(1, 11)
                    header_df = pd.DataFrame(temp,
                                             columns=["seq_id","DocumentId","AccesssionNumber","FundName","CompanyName","SecurityId","MeetingDate","MeetingType",
                                       "ISIN","Ticker","AgendaNumber"])
                    main_header_df = main_header_df.append(header_df)
                    #print(data)
                    #print()
                elif i % 2 != 0:
                    #print("<------Table Data----->")
                    for val in data:
                        row_list = list()
                        row_list.append(seq_id)
                        if not isinstance(val, int):
                            # val = re.sub(r"")
                            split = re.split(r"\t", val)
                            for value in split:
                                row_list.append(value.lstrip())
                        else:
                            seq_id = val
                        tp = np.asarray(row_list)
                        if len(tp) <6:
                            for x in range(len(tp), 6):
                                tp = np.append(tp, None)
                        # print(tp)
                        tp = tp.reshape(1, 6)
                        # print(tp)
                        temp_df = pd.DataFrame(tp, columns=["seq_id","ProposalNumber","Proposal","ProposedBy","ForAgainstManagement","VoteCast"])
                        table_df = table_df.append(temp_df)
                    #print(data)
                    #print()
        except Exception as error:
            logger.error("Error in post-processing for accession_no-{}-{}".format(accession_no,error))
            print("Error in post-processing for accession_no-{}-{}".format(accession_no,error))
            return pd.DataFrame(),pd.DataFrame()

        return main_header_df,table_df


    @staticmethod
    def process_df(accession_no,main_header_df,table_df):
        logger = Utils.add_logger()
        try:
            table_df = table_df.astype({"seq_id": int})
            main_header_df = main_header_df.reset_index(drop=True)
            main_header_df = main_header_df.astype({"seq_id": int})
            df = table_df.dropna(subset=['Proposal'])
            df["ProposedBy"] = df["ProposedBy"].apply(Parser.replace_empty)
            df["ProposalNumber"] = df["ProposalNumber"].apply(Parser.replace_empty)
            main_header_df["MeetingDate"] = main_header_df["MeetingDate"].apply(Utils.parsing_date)
            df = df.reset_index(drop=True)
            r = len(df.index) - 1
            #print(r)
            while r >= 0:
                temp_index = list()
                while ((df.at[r, "Proposal"] != "DIRECTOR" or df.at[r,"ProposalNumber"]==None)and (df.at[r, "ProposedBy"] == None)):
                    temp_index.append(r)
                    r -= 1
                #print(temp_index)
                r -= 1
                if len(temp_index) >= 1:
                    c = temp_index[-1]
                    # print(c)
                    for m in reversed(temp_index):
                        df.at[c - 1, "Proposal"] += " " + df.at[m, "Proposal"]
                        df.at[m, "Proposal"] = None

            df = df.dropna(subset=["Proposal"])
            final_df = main_header_df.merge(df,on=["seq_id"],how="inner")
        except Exception as error:
            logger.error("Error in process_df for accession_no-{}-{}".format(accession_no,error))
            print("Error in df processing for accession_no-{}-{}".format(accession_no,error))
            return pd.DataFrame()

        return final_df




