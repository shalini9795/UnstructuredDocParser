import time
import unicodedata
# import modin.pandas as pd
from bs4 import BeautifulSoup as bs, BeautifulSoup
import re
import pandas as pd
from  Format1.config.config import CONFIG
import logging
import numpy as np
from Format1.format1_parser.utils.session_manager import engine
from datetime import datetime
import os
import lxml
import logging.config
from dateutil.parser import parse
from Format1.format1_parser.db_mapping import Parsed_record
import Format1.format1_parser.utils.dbops_dao as DB
from Format1.format1_parser.dao_impl import Dao_impl
# from Format1.format1_parser.utils.context_manager import session_scope
os.environ["MODIN_ENGINE"] = "dask"

class Parser():

    @staticmethod
    def add_logger():
        logging.config.fileConfig('loggerconfig.conf')
        logger = logging.getLogger('MainLogger')
        fh = logging.FileHandler(CONFIG["Path"]["log_path"]+'{:%Y-%m-%d}.log'.format(datetime.now()))
        formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(lineno)04d | %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        return logger

    @staticmethod
    def replace_space(str):
        str = str.replace('\xa0', '\n').strip()
        str = str.replace('\ufeff', '\n')  ## to handle hidden space
        str = str.replace('\\amp', '\n')
        return str

    @staticmethod
    def normalize_specialchar(text):
        return ''.join(c for c in unicodedata.normalize('NFKD', text) if unicodedata.category(c) != 'Mn')

    # @staticmethod
    # def parsing_date(text):
    #     text = text.lstrip(' ').rstrip(' ').strip('\n')
    #     # print("in date",text)
    #     if not text:
    #         # print("date is ",text)
    #         return text
    #
    #     for fmt in ('%b %d, %Y', '%Y-%m-%d', '%d.%m.%Y', '%d %B, %Y', '%B %d,%Y'):
    #         try:
    #             return datetime.strptime(text, fmt)
    #         except Exception as e:
    #             print("Exception in date conversion", e)
    #             return text

    @staticmethod
    def parsing_date(text):
        text = text.lstrip(' ').rstrip(' ')
        # print("in date",text)
        try:
            date = parse(text)
        except Exception as error:
            logging.error("Exception in date parsing:{}".format(error))
            print("Exception in date parsing:{}".format(error))
            return text
        return date

    @staticmethod
    def replace_empty(text):
        try:
            if len(text)==0:
                return None
            else:
                return  text
        except:
            logger=Parser.add_logger()
            logger.debug("Exception in replacing empty in replace_empty method")


    @staticmethod
    def findFund(s):
        p = re.compile("={4,}\s{1,}[A-Za-z\.\-\,\&\d\s+]*=*")
        # p = re.compile("={4,}\s{1,}[A-Za-z\.\-\,\& \d\s]*=*") #earlier used this
        l = []
        str1 = ''
        for m in p.finditer(s):
            str1 = str(m.start()) + '  ' + m.group()
            #         print(m.start(), m.group())
            l.append(str1)
        l = [x for x in l if "END NPX REPORT" not in x]
        return l

    @staticmethod
    def putDelimiter(my_str):
        fund = Parser.findFund(my_str)
        if (len(fund) != 0):
            val = fund[0].split('  ')
            val = int(val[0])
            my_str = my_str[val:]
        try:
            substr = "#     Proposal                                Mgt Rec   Vote Cast    Sponsor"
            # substr=re.compile(r"\s*#\s+Proposal\s+Mgt\s+Rec\s+Vote\s+Cast\s+Sponsor\n")
            # substr=re.compile("Sponsor\n")
            inserttxt = "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^"
            idx = my_str.index(substr)
            my_str = my_str[:idx] + inserttxt + my_str[idx:]
        except Exception as e:
            logger = Parser.add_logger()
            logger.debug("Exception in replacing empty")
            print("Exception in search is",e)
            return 'Empty'
        return my_str

    @staticmethod
    def checkFundEmpty(total):
        mystrlist = Parser.putDelimiter(total).split(
            "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
        if "Empty" in (mystrlist[0]):
            return True
        else:
            return False

    @staticmethod
    def readFile(accession_no):
        path = CONFIG["Path"]["path"] + accession_no + ".dissem"
        try:
            file = open(path, 'r', encoding='utf-8')
            s = file.read()
            fileFormat12 = BeautifulSoup(s, "lxml")
            reData = fileFormat12.find('pre')
            # print("reData is",reData)
            if(reData is None):
                reData=fileFormat12.find("page")
                s=str(reData).split("FORM N-Px REPORT")[1]
            else:
                s = str(reData)
            s = s.replace('&amp;', '&')
            s = s.replace('&quot;', '&')
            s = s.replace('========== END NPX REPORT', '')
            s = s.replace('no proxy voting activity','---------------------------------------------------------------------------\n')
            s = s.replace('</pre>', '')
            s=s.replace(('</page>'),'')
            # s=str(s.encode('ansi'))
            # s.replace("\xB7","")
            # s=str(s.encode("utf-8"))
            # s=Parser.replace_space(s)
            # print("normalised character",Parser.normalize_specialchar(s))
        except Exception as e:
            logger = Parser.add_logger()
            logger.debug("Exception")
            logger.error("File not found exception {}".format(e))
            print("File not found exception ",e)
        return s

    @staticmethod
    def companyNullHandle(t):
        for i in range(len(t)):
          if "==" not in t[i] and t[i].isupper()==True:
              company=t[i]
              break
        ticker_index=t.index("Ticker:")
        if("Security ID:" in t[ticker_index+1]):
            ticker=""
        else:

            ticker = t[ticker_index + 1]
        return company,ticker

    @staticmethod
    def trimSpaces(text):
        text=text.lstrip().rstrip()
        return text

#merge acc to seq id
    @staticmethod
    def mergeFunds(fund):
        name=''
        try:
            for a in range(len(fund)):
                val = fund[a].split('  ')
                val = (val[1]).strip("=").rstrip()
                name = name + ' ' + val
        except:
            logger = Parser.add_logger()
            logger.debug("Exception in replacing empty")
            print("error in merging funds")
        return name

    @staticmethod
    def headerFundsList(s):
        pattern = re.compile(r'(\s){3,}')
        list_header = re.split('-{5,}', s)
        list_header = list(filter(None, list_header))
        # print("list_header is",list_header)
        # print("length",len(list_header))
        funds_list = []
        seq_id = 1
        fund = ''
        previousfund = ''
        company = ''
        ticker = ''
        print("list header length is", len(list_header))
        for i in range(len(list_header)):
            #     a = re.search(r'Mgt Rec', list_header[i])
            #     print(a)
            # look for proposal in this whole string and then
            # put ********** delimiter before the proposal line
            # then split it on base of delimiter and assign the same no to header and table.

            #     print(list_header[i],"********\n"
            total = list_header[i]
            # print("Empty",checkFundEmpty(total))
            header_list = []
            # print("total is",total)
            if (Parser.checkFundEmpty(total) == True):
                continue
                # try:
                #     fund = Parser.findFund(list_header[i])
                #     if (len(fund) != 0):
                #         val = fund[0].split('  ')
                #         fund = val[1].strip("=").lstrip().rstrip()
                #         header_list.append('')
                #         header_list.append('')
                #         header_list.append('')
                #         header_list.append('')
                #         header_list.append(fund)
                #         header_list.append('')
                #         header_list.append('')
                #         header_list.append(seq_id)
                # except:
                #     print("Exception in checking empty fund votes")

            try:
                mystrlist = Parser.putDelimiter(total).split(
                    "^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
                # print("to fix fund-------------------------------------------",mystrlist[0])
                list_delimiter = re.sub('\r', '\n', mystrlist[0])
                list_delimiter = re.sub(pattern, '\n', mystrlist[0])
                # list_delimiter=re.sub('\r','\n',mystrlist[0])   #to handle count mismatch
                mystrlist1 = list_delimiter.split('\n')
                # print("mystrlist1 is \n", mystrlist1)
                for i, s in enumerate(mystrlist1):
                    #     print(s)
                    if "Security ID:" in s:
                        #         print("s isss",s)
                        t = s.split(':')
                        try:
                            security = t[1].lstrip()
                            # print("Security: is", security)
                            header_list.append(security)
                        except:
                            print("error in finding security id")
                    elif "Meeting Type:" in s:
                        t = s.split(':')
                        try:
                            meeting_type = t[1].lstrip()
                            # print("Meeting_type:", meeting_type)
                            header_list.append(meeting_type)
                        except:
                            print("error in finding meeting type")
                    elif "Meeting Date:" in s:
                        t = s.split(':')
                        try:
                            meeting_date = t[1].lstrip()
                            # print("Meeting_date:", (meeting_date))
                            header_list.append(meeting_date)
                        except:
                            print("error in finding meeting date")
                    elif "Record Date:" in s:
                        t = s.split(':')
                        try:
                            record_date = t[1].lstrip()
                            # print("Meeting_date:", (record_date))
                            header_list.append(record_date)
                        except:
                            print("error in finding record date")
                    elif "Ticker:" in s:
                        # print(s)
                        try:
                            t = list_delimiter.split('\n')
                            t = list(filter(None, t))
                            check = t[0]
                            #         print("t is",t)
                            ticker = t[2].lstrip()
                            if (len(Parser.findFund(check)))!=0:
                                s = Parser.findFund(list_delimiter)
                                # s = s[0].split('  ')
                                fund=Parser.mergeFunds(s)
                                # print("funds is",fund)
                                # fund = s[1].strip("=").strip(' ')
                                header_list.append(fund)
                                previousfund = fund
                                # company = t[1]
                                # ticker = t[3].lstrip()
                                company,ticker=Parser.companyNullHandle(t)
                                header_list.append(company)
                                header_list.append(ticker)
                                # print("Fund:", fund)
                                header_list.append(seq_id)
                            else:
                                company = t[0]
                                header_list.append(previousfund)
                                company, ticker = Parser.companyNullHandle(t)
                                header_list.append(company)
                                header_list.append(ticker)
                                header_list.append(seq_id)
                            # print("Company:", company)
                            # print("Ticker:", ticker)
                        except Exception as e:
                            print("error in trigger segment",e)

                funds_list.append(header_list)
                # print("size of funds_list",len(funds_list))
            except:
                print("here in except block")
                fund = Parser.findFund(s)
                if (len(fund) != 0):
                    val = fund[0].split('  ')
                    val = val[1]
                    header_list.append(val)
                header_list.append("Empty")
                funds_list.append(header_list)

            try:
                list_delimiter1 = re.sub(pattern, '\n', mystrlist[1])
                list_delimiter1 = re.sub("#     Proposal                                Mgt Rec   Vote Cast    Sponsor",
                                         '', mystrlist[1])
                # print(list_delimiter)
                mystrlist2 = list_delimiter1.split('\n')
                mystrlist2 = list(filter(None, mystrlist2))
                # print("mystrlist2 is \n", mystrlist2)
                table_list = list()
                for x, spli in enumerate(mystrlist2):
                    row_list = list()
                    # table_list.append(seq_id)
                    if len(spli) > 0:
                        temp_text = ""
                        temp_text += previousfund
                        temp_text += "\t" + spli[0:6].strip()
                        temp_text += "\t" + spli[6:46].strip().strip("\n")
                        temp_text += "\t" + spli[46:56].strip().strip("\n")
                        temp_text += "\t" + spli[56:69].strip().strip("\n")
                        temp_text += "\t" + spli[69:].strip().strip("\n")
                        temp_text += "\t" + str(seq_id)
                        # row_list.append(temp_text.split("\t"))
                        # print("row_list is ",row_list)
                        table_list.append(temp_text)
                        # print(table_list)
                        # print(temp_text)
                funds_list.append(table_list)
                seq_id = seq_id+1
                # sep = re.split("\s{3,}", spli)
                # print(table_list)
            except Exception as e:
                logger = Parser.add_logger()
                logger.info("exception in parsing table data")
                logger.error("exception in parsing table data{}".format(e))
        return funds_list

    @staticmethod
    def savetodataframe(funds_list):
        logger=Parser.add_logger()
        # funds_list = Parser.headerFundsList()
        main_header_df = pd.DataFrame(
            columns=["FundName", "CompanyName", "Ticker", "seq_id", "SecurityId", "MeetingDate", "MeetingType",
                     "RecordDate"])
        table_df = pd.DataFrame(
            columns=["fund_name", "ProposalNumber", "Proposal", "ManagementRecommendation", "VoteCast", "ProposedBy", "seq_id"])
        for i, data in enumerate(funds_list):
            if i % 2 == 0:
                temp = np.asarray(data)
                temp = temp.reshape(1, 8)
                header_df = pd.DataFrame(temp,
                                         columns=["FundName", "CompanyName", "Ticker", "seq_id", "SecurityId", "MeetingDate", "MeetingType",
                     "RecordDate"])
                main_header_df = main_header_df.append(header_df)
                try:
                    main_header_df = main_header_df.astype(
                        {"seq_id": int})  # changing type of sequence id to int in header data
                except Exception as e:
                    logger.error("error in changing type {}".format(e))
                    print("error in changing type", e)

            elif i % 2 != 0:
                for val in data:
                    row_list = list()
                    split = re.split(r"\t", val)
                    for value in split:
                        row_list.append(value.lstrip())
                    tp = np.asarray(row_list)
                    if len(tp) < 6:
                        for x in range(len(tp), 6):
                            tp = np.append(tp, None)
                    try:
                        tp = tp.reshape(1, 7)
                    except Exception as e:
                        logger.error("Exception in reshaping{}".format(e))
                        print("exception in reshaping",e)
                    temp_df = pd.DataFrame(tp, columns=["fund_name","ProposalNumber", "Proposal", "ManagementRecommendation", "VoteCast", "ProposedBy", "seq_id"])
                    table_df = table_df.append(temp_df)
                    table_df = table_df.astype({"seq_id": int})  # changing the type of seq_data to int in table data

        return main_header_df,table_df

    @staticmethod
    def collapse(main_header_df,table_df):
        logger=Parser.add_logger()
        df = table_df.dropna(subset=['Proposal'])
        df = df.reset_index(drop=True)
        df["ProposalNumber"] = df["ProposalNumber"].apply(Parser.replace_empty)
        df["ManagementRecommendation"]=df["ManagementRecommendation"].apply(Parser.replace_empty)
        r = len(df.index) - 1
        logger.info("Concatenation start at{}".format(datetime.now()))
        # while r >= 0:
        start_time = time.time()
        # temp_index=[r for row in df.iterrows()  if df.at[r, "Proposal"] != None and df.at[r, "ProposalNumber"] == None and df.at[r,"ManagementRecommendation"]==None]
        # print(temp_index)

        for r, row in df.iterrows():
            temp_index = list()
            if df.at[r, "Proposal"] != None and df.at[r, "ProposalNumber"] == None and df.at[r,"ManagementRecommendation"]==None: #changes acc to mgmt proxy
                temp_index.append(r)

                # print(r)
                # if not(df.at[r-c,"proposal_type"] != None):
                # temp_index.append(r-c)
            # print(temp_index)
            # r -= 1
            if len(temp_index) >= 1:
                c = temp_index[-1]
                # print(c)
                for m in reversed(temp_index):
                    df.at[c - 1, "Proposal"] += " " + df.at[m, "Proposal"]
                    df.at[m, "Proposal"] = None
        end_time = time.time()
        total_time = end_time - start_time
        print("Time taking for concat: ", total_time)
        logger.info("Concatenation complete at{}".format(datetime.now()))
        df = df.dropna(subset=['Proposal'])
        logger.info("merge start at{}".format(datetime.now()))
        start_time = time.time()
        test_df = main_header_df.merge(df, on=["seq_id"], how="inner")
        end_time = time.time()
        total_time = end_time - start_time
        print("Time taking for merge: ", total_time)
        logger.info("merge end at{}".format(datetime.now()))
        return test_df

    @staticmethod
    def df_to_database(accession_no,documentId,final_df):
        logger = Parser.add_logger()
        try:
            print("col here is",final_df.columns)
            final_df = final_df.drop(columns=["seq_id"])
            try:
                final_df["MeetingDate"] = final_df["MeetingDate"].apply(Parser.parsing_date)
                final_df["FundName"]=final_df["FundName"].apply(Parser.trimSpaces)
                # final_df["RecordDate"] = final_df["RecordDate"].apply(Parser.replace_empty())
                final_df["RecordDate"] = final_df["RecordDate"].apply(Parser.parsing_date)
            except Exception as e:
                logger.error("error in date converson for{}".format(e))
                print("error in date converson for")
            final_df.drop('fund_name', axis=1, inplace=True)
            print("columns check1", final_df.columns)
            # final_df.set_index('FundName', inplace=True)
            final_df = final_df.drop_duplicates()
            print("columns check",final_df.columns)
            filename = os.path.join(CONFIG["Path"]["output_path"], accession_no + ".xlsx")
            # final_df.replace('=', '0', regex=True)
            # final_df = final_df.dropna(how='any', axis=1)
            final_df['AccesssionNumber'] = accession_no
            final_df['DocumentId'] = documentId
            final_df['CreatedBy']=0
            print("columns are",final_df.columns)
            # params=(accession_no,documentId)
            # tvp = pytds.TableValuedParam(type_name='StringTable',rows = ((x) for x in params))
            # print(tvp)
            # engine.execute('EXEC DeleteParsedRecord %s', (tvp,))
            # cursor.execute(DeleteParsedRecord(?,?))
            logger.info("Before connection establish for db")
            conn = engine.raw_connection()
            doc_id = int(documentId)
            cursor = conn.cursor()
            res = cursor.execute('exec DeleteParsedRecord ?,?', accession_no, doc_id)
            logger.info("After stored proc for delete record")
            cursor.close()
            conn.commit()
            logger.info("Before saving in db for accession no{}".format(accession_no))
            final_df[~final_df.Proposal.str.contains("=")].to_sql(name="ParsedRecords", con=engine, if_exists='append', index=False)
            logger.debug("bulk insert successful for accession no{}".format(accession_no))
            logging.debug("bulk insert into Fund_Vote table")
            print("bulk insert into Fund_Vote table successfull")
        except Exception as error:
            logger.error("Insertion into database failed for accession_no:{}-{}".format(accession_no,error))
            #logging.error("Insertion into database failed for accession_no:{}-{}".format(accession_no,error))
            print("Insertion into database failed for accession_no:{}-{}".format(accession_no,error))
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def df_to_excel(accession_no,final_df,documentId=0):
        logger=Parser.add_logger()
        try:
            final_df = final_df.drop(columns=["seq_id"])
            final_df.drop('fund_name', axis=1, inplace=True)
            final_df.set_index('FundName', inplace=True)
            final_df = final_df.drop_duplicates()
            filename = os.path.join(CONFIG["Path"]["output_path"], accession_no + ".xlsx")
            # final_df.to_excel(filename)
            final_df.replace('=', '0', regex=True)
            final_df = final_df.dropna(how='any', axis=1)
            no="none"
            final_df["Notes"]=no
            # m = final_df["MeetingType"].any.str.contains('Proxy Contest') == True and final_df["Proposal"].str.contains(
            #     "Proxy") == True
            # print(m.type)
            # mask=final_df["MeetingType"].str.contains('Proxy Contest') == True
            # final_df["Notes"][mask] = final_df['Proposal'][mask]
                # final_df["Proposal"][mask]=""
            # final_df['e'] = pd.Series(np.random.randn(sLength), index=final_df.index)
            final_df['AccesssionNumber'] = accession_no
            final_df['DocumentId']=documentId
            final_df[~final_df.Proposal.str.contains("=")].to_excel(filename)
        except Exception as error:
            logger.error("Saving to excel failed for acession_no:{}-{}".format(accession_no, error))
            print("Saving to excel failed for acession_no:{}-{}".format(accession_no, error))