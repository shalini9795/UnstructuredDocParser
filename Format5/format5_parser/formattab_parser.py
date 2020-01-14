from datetime import datetime
from db_ops.session_manager import engine
from bs4 import BeautifulSoup
import glob
import os
import re
import contextlib
import pandas as pd
from config.config import CONFIG
from utils.utility import Utils
#Format 5
# from Format5.format5_parser.utils.session_manager import engine


class ParserFormat5:

    @staticmethod
    def read_file(accession):
        path = CONFIG["Path"]["file_path"] + accession + ".dissem"
        try:
            f = open(path, 'r')
            contents = f.read()
        except IOError as e:
            print('File not found')
        except:
            try:
                f = open(path, 'r', encoding="utf-8")
                contents = f.read()
            except:
                print('File error')
        return contents

    # @staticmethod
    # def tabledetails(table, fundname):
    #     line = []
    #     temp = False
    #     length = 0
    #     str = ''
    #     table_rows = table.find_all('tr')
    #     dol=pd.DataFrame()
    #     try:
    #         for tr in table_rows:
    #             th = tr.find_all('th')
    #             for i in th:
    #                 if len(i.text.strip()) > 0:
    #                     line.append(i.text)
    #
    #             td = tr.find_all('td')
    #             for i in td:
    #                 if len(i.text.strip()) > 0:
    #                     line.append(i.text)
    #             # print(line)
    #             if temp == False:
    #                 # print(line)
    #                 temp = ParserFormat5.fundData(line)
    #                 if temp == True:
    #                     df = pd.DataFrame(columns=line)
    #                     col = line
    #                     # print()
    #                     # print("line is", line)
    #                     length = len(line)
    #                 else:
    #                     for i in line:
    #                         str = str + i
    #             else:
    #                 if length == len(line):
    #                     df = df.append(pd.Series(line, index=col), ignore_index=True)
    #
    #             line = []
    #         if temp == False:
    #             df = pd.DataFrame()
    #             dol = ParserFormat5.companydata(str)
    #
    #     except Exception as e:
    #         print("Exception in tabledetails",e)
    #     print(dol.empty)
    #     # print(df.empty)
    #
    #     try:
    #         if dol.empty != True and df.empty != True:
    #             dol['ID'] = 1
    #             df['ID'] = 1
    #             # print('--------------- !')
    #             print("----------------------------------------------------------------------\n")
    #             print("****",dol)
    #             df_all_rows = pd.merge(dol, df, on=['ID'])
    #             df_all_rows['FundName'] = fundname
    #             # print(df_all_rows)
    #             print("col of merged df is", df_all_rows.columns)
    #             return df_all_rows
    #     except Exception as e:
    #         print("Exception in all rows",e)
    #         # print('---------------------@')
    @staticmethod
    def parsing_date(text):
        text = text.lstrip(' ').rstrip(' ').strip('\n')
        # print("in date",text)
        if not text:
            print("date is ", text)
            return text

        for fmt in ('%b %d, %Y', '%Y-%m-%d', '%d.%m.%Y', '%d %B, %Y', '%B %d,%Y'):
            try:
                return datetime.strptime(text, fmt)
            except Exception as e:
                print("Exception in date conversion", e)
                return text

    @staticmethod
    def tabledetails(table, fundname):
        line = []
        temp = False
        length = 0
        str = ''
        table_rows = table.find_all('tr')
        for tr in table_rows:
            th = tr.find_all('th')
            for i in th:
                if len(i.text.strip()) > 0:
                    line.append(i.text)

            td = tr.find_all('td')
            for i in td:
                if len(i.text.strip()) > 0:
                    line.append(i.text)
            # print(line)
            if temp == False:
                # print(line)
                temp = ParserFormat5.fundData(line)
                if temp == True:
                    df = pd.DataFrame(columns=line)
                    col = line
                    # print()
                    length = len(line)
                else:
                    for i in line:
                        str = str + i
            else:
                if length == len(line):
                    df = df.append(pd.Series(line, index=col), ignore_index=True)
            line = []
        if temp == False:
            df = pd.DataFrame()
        dol = ParserFormat5.companydata(str)
        # print(dol.empty)
        # print(df.empty)

        if dol.empty != True and df.empty != True:
            dol['ID'] = 1
            df['ID'] = 1
            # print('--------------- !')
            df_all_rows = pd.merge(dol, df, on=['ID'])
            df_all_rows['FundName'] = fundname
            # print("columns", df_all_rows.columns)
            # print('---------------------@')
        return df_all_rows

    @staticmethod
    def identify_header(str):
        header_list = ['proposal no']
        temp = False
        for list in header_list:
            if str.strip().lower().find(list) > -1:
                temp = True
                break
        return temp

    @staticmethod
    def fundData(strlist):
        temp = False
        for i in strlist:
            temp = ParserFormat5.identify_header(i)
            if temp == True:
                break
        return temp

    @staticmethod
    def companydata(str):
        print(str)
        str = re.sub('\xa0+', ' ', str)
        str = re.sub('\n+', '|', str)
        str = re.sub('\|+', '|', str)
        str = 'ISSUER:' + str
        try:
            list = str.split('|')
            ##print(list)
            header = ['Header', 'Value']
            df = pd.DataFrame(columns=header)
            for col in list:
                collist = col.split(':')
                # print("collist is", collist)
                if len(collist) > 1:
                    # print("collist is hereee", collist)
                    df = df.append(pd.Series(collist, index=header), ignore_index=True)
            # print(df)
            dt = df.set_index('Header').transpose()
            # print("columns of header is",dt.columns)
            # print("columns of df is",df.columns)
        except Exception as e:
            print("Exception in companyData",e)
        # print(dt)
        return dt

    @staticmethod
    def preprocess(accession_no):
        content=ParserFormat5.read_file(accession_no)
        content = content.replace('<br>', '|')
        content = content.replace('</tr>\n<td', '</tr><tr><td')
        fundVoteData = content.split('VOTE SUMMARY REPORT')
        return fundVoteData

    @staticmethod
    def mergeDataframe(accession_no):
        fundVote=ParserFormat5.preprocess(accession_no) #return string
        try:
            df = pd.DataFrame()
            df1 = pd.DataFrame()
            print("length of fundvote is",len(fundVote))
            if (len(fundVote) != 0):
                for list in fundVote:
                    # print("list is",list)
                    soup = BeautifulSoup(list, "lxml")
                    tables = soup.findAll('table')
                    print("no of tables",len(tables))
                    fundName = list.split('\n')
                    # print("length of fundname",len(fundName))
                    for table in tables:
                        # print(table)
                        try:
                            df1=ParserFormat5.tabledetails(table, fundName[1])
                            df = df.append(df1)
                        except Exception as e:
                            print("Exception is ",e)
                    print("appended", df)
                    print("type of df is", type(df))
            return df
        except Exception as e:
            print("exception in merge",e)
            return df

    @staticmethod
    def df_to_excel(accession_no, final_df):
        try:
            print("type received for excel",type(final_df))
            path=CONFIG["Path"]["output_path"]
            filename = path +accession_no+ ".xlsx"
            print("location is",filename)
            final_df.to_excel(filename)
            print("saved to excel")
        except Exception as e:
            print("Saving to excel failed for acession_no:{}".format(accession_no))
            print("Exception in saving to excel is",e)

    @staticmethod
    def replacespecial(text):
        text=text.strip('|')
        return text

    @staticmethod
    def df_to_sql(accession_no,documentId,final_df):
        try:
            print("renames columns")
            final_df.rename(columns={'ISSUER': 'CompanyName', 'MEETING DATE': 'MeetingDate',
                                        'TICKER': 'Ticker',' SECURITY ID':'SecurityId','Proposal No':'ProposalNumber',
                                     'Proposed By':'ProposedBy','Management Recommendation':'ManagementRecommendation',
                                     'Vote Cast':'VoteCast','Security ID': 'SecurityId'}, inplace=True)
            final_df.drop('ID', axis=1, inplace=True)
            print("columns are", final_df.columns)
            columnslist = []
            for column in final_df.columns:
                column = column.replace(" ", "")
                columnslist.append(column)
            final_df.columns = columnslist
            final_df['FundName']=final_df["FundName"].apply(ParserFormat5.replacespecial)
            final_df["MeetingDate"] = final_df["MeetingDate"].apply(Utils.parsing_date)
            print("columns are", final_df.columns)
            conn = engine.raw_connection()
            doc_id = int(documentId)
            cursor = conn.cursor()
            res = cursor.execute('exec DeleteParsedRecord ?,?', accession_no, doc_id)
            cursor.close()
            conn.commit()
            final_df['AccesssionNumber'] = accession_no
            final_df['DocumentId'] = doc_id
            final_df.to_sql(name="ParsedRecords", con=engine, if_exists='append',
                                                                  index=False)
            print("saved to db")
        except Exception as e:
            print("error saving in database",e)
        finally:
            if conn is not None:
                conn.close()

