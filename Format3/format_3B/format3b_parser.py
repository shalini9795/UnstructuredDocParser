import collections
import numpy
from bs4 import BeautifulSoup
import glob
import os
import re
import contextlib
import pandas as pd
import logging.config
from db_ops.session_manager import engine

from Format3.format_3B.config.config import CONFIG
# from utils.utility import Utils
# from utils.utility import Utils


class Format3Parser:

    @staticmethod
    def read_file(accession_no):
        try:
            path = CONFIG["Path"]["path"] + accession_no + ".dissem"
            print("path",path)
            f = open(path, 'r')
            contents = f.read()
        except IOError as e:
            print('File not found')
        except:
            try:
                f = open(path, 'r', encoding="utf-8")
                contents = f.read()
                content = contents.replace('</TD> <TR', '</TD></TR> <TR')
                soup = BeautifulSoup(content, "html.parser")
            except:
                print('File error')
        return contents

    @staticmethod
    def tabledetails(table, FilePath):
        # print(table)
        NA = ['N/A']
        companyName = ''
        Name = ''
        line = []
        sline = []
        temp = False
        ctemp = False
        cheader = False
        df = pd.DataFrame()
        dfc = pd.DataFrame()
        head = 0
        hline = []
        vhline = []
        length = 0
        fundName = ''
        col = []
        df_all = pd.DataFrame()
        ij = 0
        try:
            table_rows = table.find_all('tr')
            print("no of tr for one table", len(table_rows))
            count = len(table_rows)
        except Exception as e:
            print("Exception in finding tr", e)
        try:
            previousFund = ''
            for l, tr in enumerate(table_rows):
                td = tr.find_all('td')
                for i in td:
                    if len(i.text.strip()) > 0:
                        line.append(i.text.strip('\n'))
                    sline.append(i.text.strip('\n'))
                    # print("line is",line)

                # line = list(filter(None, line))
                # sline = line
                head=Format3Parser.gettableinheader(line)
                if temp == False:
                    temp = Format3Parser.identify_header(line)
                    if temp == True:
                        head = 1
                elif line == [] or l + 1 == count:
                    head = 0
                    print("Count variable is", count)

                    # print("line variable
                    # sent to identify fund name is",line)
                    # line = list(filter(None, line))
                    if temp == True and head == 0 and length == len(line):
                        df = df.append(pd.Series(line, index=col), ignore_index=True)
                    temp = False
                    ctemp = False;
                    cheader = False;
                    dsa = Format3Parser.fdataSet(fundName, hline, vhline, df, companyName)
                    # values are coming here
                    df_all = pd.concat([df_all, dsa], ignore_index=True)
                    vhline = []
                    hline = []
                    cola = []
                else:
                    head = 0
                print("head is ", head)
                if temp == True and head == 1:
                    df = pd.DataFrame(columns=line)
                    print("line before header", line)
                    col = line
                    length = len(col)
                elif temp == True and head == 0 and length == len(line):
                    df = df.append(pd.Series(line, index=col), ignore_index=True)
                # elif temp==True and head==0 and length!=len(line) and "Non-Voting Meeting Note" in line:
                #     numberofNA=length-len(line)
                #     for i in range(numberofNA):
                #         line.extend(NA)
                else:
                    df = pd.DataFrame()
                    if line != []:
                        try:
                            if "Non-Voting Meeting Note" in line:
                                numberofNA = length - len(line)
                                for i in range(numberofNA):
                                    line.extend(NA)
                                    print(line)
                                df = df.append(pd.Series(line, index=col), ignore_index=True)
                            # if "N/A" in line or "For" in line or "Mgmt" in line or
                            print("line sent to identify fund name 2", line)
                            f = Format3Parser.identify_fundName(line)
                            f = f.strip("\n").strip(" ").lstrip().rstrip()
                            # print("Fund name returned", Name)
                            # indices = line.split(',')
                            # print(indices)
                            # print(type(indices))
                            if len(line) == 1 and "Fund" not in line:
                                companyName = line[0]
                            if f and not f.isspace():  # to check if str not empty
                                # print("true")
                                Name = f
                                previousFund = Name
                            # print("curr fund",f)
                            else:
                                # print("false")
                                Name = previousFund
                            # print("prev fund",previousFund)
                            ctemp = Format3Parser.identify_Companyheader(line)
                        except Exception as e:
                            print("Error in identify company or fund ", e)

                        try:
                            if ctemp == True:
                                hline = hline + sline
                                ctemp = False
                                cheader = True
                            elif cheader == True:
                                vhline = vhline + sline
                            if Name != '':
                                fundName = Name

                        except Exception as e:
                            print("Exception in hline  vline", e)
                        # if "kao corporation" in line[0].lower():
                        #     print("vline", vhline)
                        #     print("hline",hline)

                        # print()
                # print("line values total",line)
                line = []
                sline = []
                ij = ij + 1
                # print("ij is",ij)
                # print("hline is",hline)
                # print("vline is",vhline)
                # print("previous fund is")
        except Exception as e:
            print("table details Exception is", e)
            # print(df_all.head())

        return df_all

    @staticmethod
    def returnTables(contents):
        contents = contents.replace('</TD> <TR', '</TD></TR> <TR')
        soup = BeautifulSoup(contents, "html.parser")
        tables = soup.findAll('table')
        return tables

    @staticmethod
    def mergedf(tables,filePath=0):
        df_all_dc = pd.DataFrame()
        df = pd.DataFrame()
        previousFund=''
        print("No of tables",len(tables))
        try:
            x = Format3Parser.returntableindex(tables[0])
        except:
            x=1
        try:
            for table in tables[0:]:
                dfac1 = Format3Parser.tabledetails(table, filePath)
                # print(pd.isnull(dfac1.loc[1,'FundName']))
                # if pd.isnull(dfac1.loc[1,'FundName'])==True:
                #     dfac1['FundName']=previousFund
                #     print(dfac1.head())
                # elif pd.isnull(dfac1.loc[1,'FundName'])==False:
                #     previousFund=dfac1.loc[1,'FundName']
                #
                #     print("merge pv fund",previousFund)
                # df_all_dc = pd.concat([df_all_dc, dfac1], ignore_index=True)
                try:
                    df = df.append(dfac1)
                    # print(df)
                except Exception as e:
                    print("Exception in append",e)
                    print("size of df is", df.size)
            return df
        except Exception as e:
            print("Exception in merge is",e)

        return df

    @staticmethod
    def df_toexcel(tables,accession_no,documentId=0,filePath=0):
        try:
            path = os.path.join(CONFIG["Path"]["output_path_format3B"], accession_no + '.xlsx')
            df=Format3Parser.mergedf(tables,filePath=0)
            print("columns is",df.columns)
            df['AccessionNumber']=accession_no
            # df['DocumentId']=documentId
            print("size is", df.size)
            if "ID" in df.columns:
                df = df.drop(columns=["ID"])
            if "CountryofTrade" in df.columns:
                df=df.drop(columns=['CountryofTrade'])
            if "Country of Trade" in df.columns:
                df=df.drop(columns=['Country of Trade'])
            df = df.replace(numpy.nan, '', regex=True)
            df[df == ''] = numpy.nan;
            df = df.dropna(axis=1, how='all')
            # df=df.dropna(how='any', axis=1)
            print("columns is", df.columns)
            df.to_excel(path)
            print("Saved to excel successful")
        except Exception as e:
            print("Exception in py to excel ",e)


    @staticmethod
    def df_tosql(df,accession_no,documentId,filePath=0):
        try:
            doc_id=int(documentId)
            df['AccesssionNumber'] = accession_no
            df['DocumentId'] = documentId
            df['CreatedBy'] = 0
            df.drop(df.columns[[0]], axis=1, inplace=True)
            del df['ID']
            # df.drop(df.columns[[7]], axis=1, inplace=True)
            print(df.columns)
            df.rename(columns={'Meeting Date': 'MeetingDate','Security ID:': 'SecurityId', 'Issue No.': 'ProposalNumber',
                                      'Description':'Proposal','Meeting Status':'MeetingStatus','Meeting Type':'MeetingType',
                                     'Proponent': 'ProposedBy','Mgmt Rec':'ManagementRecommendation',
                                    'Country of Trade':'CountryTrade',
                                     'For/Agnst Mgmt': 'ForAgainstManagement',
                                     'Vote Cast': 'VoteCast'}, inplace=True)
            from utils.utility import Utils
            df["MeetingDate"]=df["MeetingDate"].apply(Utils.parsing_date)
            conn = engine.raw_connection()
            cursor = conn.cursor()
            res = cursor.execute('exec DeleteParsedRecord ?,?', accession_no, doc_id)
            cursor.close()
            conn.commit()
            df.to_sql(name="ParsedRecords", con=engine, if_exists='append',
                            index=False)
            print("saved to db")
        except Exception as e:
            print("Exception in saving to db",e)




    @staticmethod
    def returntableindex(data):
        data=data.lower()
        if "fund" in data or "fund name:" in data:
            return 0
        else:
            return 1

    @staticmethod
    def gettableinheader(line):
        if line.count('For')> 1 or line.count('Against')>1 or line.count('N/A')>2:
            header=True
        return 1

    @staticmethod
    def fdataSet(fundname, hlist, vlist, df,companyName):
        df_all_rows=pd.DataFrame()
        del_ind=[]
        unwanted_files = ('country', 'Country of Trade', 'Country')
        try:
            for i, header in enumerate(vlist):
                vlist[i] = vlist[i].replace(" ","").lstrip().rstrip().strip(" ")
                # code to fix point no 9
                # if ("kao corporation" in vlist[i]):
                #     print(vlist[i])
            ##########################################################################
            for i,header in enumerate(hlist):
                hlist[i] = hlist[i].replace(" ", "").lstrip().rstrip().strip(" ")
            # print("vlist before",vlist)
            if (len(hlist) != len(vlist)):
                for i, header in enumerate(vlist):
                    # print("header",header)
                    if header.lower().find("meeting")>-1 or "country" in header.lower():
                        hlist.append(vlist[i])
                        del_ind.append(i)
                        vlist[i]=vlist[i].replace(" ","").lstrip().rstrip().strip()
                        hlist[i]=hlist[i].replace(" ","").lstrip().rstrip().strip()
                        del vlist[i]
                        hlist.append("Country of Trade")
                try:
                    if "Country of Trade" in vlist:
                        vlist.remove("Country of Trade")
                    [l for l in vlist if l not in unwanted_files]
                except:
                    print("error in removing header from vlist")
                # print("changed vlist", vlist)

            dfc = pd.DataFrame(columns=hlist)
            try:
                # print("vlist", vlist)
                # print("hlist", hlist)

                dfc = dfc.append(pd.Series(vlist, index=hlist), ignore_index=True)
            except Exception as e:
                dfc=dfc.columns[dfc.columns.duplicated(keep=False)]
                print("Exception in fdataset",e)

            dfc['ID'] = 1
            df['ID'] = 1
            try:
                df_all_rows = pd.merge(dfc, df, on=['ID'])
                df_all_rows['FundName'] = fundname
                df_all_rows['CompanyName']=companyName
                print("Company name is",companyName)
                # df1=df_all_rows['CompanyName'].str.contains("Kao")
                # print(df1)
                # print("fundname is",fundname)
            except Exception as e:
                print("Exception in fdataset",e)
            # print(type(fundname))
            return df_all_rows
        except Exception as error:
            print("Exception in fdataset 2-{}".format(error))
            return df_all_rows

    @staticmethod
    def segparsing(contents):
        match1 = contents.upper().find('VOTING RECORD')
        mat = contents.upper().find('SIGNATURES')
        if mat == -1:
            mat = len(contents)
        if match1 > -1:
            report8k = contents[match1:mat]
        else:
            match1 = contents.upper().find('</HEAD>')
            if match1 > -1:
                report8k = contents[match1:mat]
            else:
                report8k = contents
        return report8k

    @staticmethod
    def identify_header(list):
        header_list = ['Description']
        temp = False
        for line in list:
            for str in header_list:
                if line.strip().find(str) > -1:
                    temp = True
                    break
                    # print(type(line))
                    # print("print header",line)
            if temp == True:
                break
        # print(temp)
        return temp

    @staticmethod
    def identify_Companyheader(list):
        header_list = ['Ticker', 'Meeting Type']
        temp = False
        for line in list:
            for str in header_list:
                if line.strip().find(str) > -1:
                    temp = True
                    break
                    # print(line)
            if temp == True:
                break
        # print(temp)
        return temp

    @staticmethod
    def identify_fundName(list):
        try:
            header='fund name'
            fundName = ''
            temp = False
            for line in list:
                if line.strip().lower().find(header) > -1:
                    temp = True
            #     # break
                if temp==True and len(list) > 1:
                    print("fundName variable value",list[1])
                    fundName = list[1]
                    break
                elif temp==True and len(list)>=0:
                    fund=list[0].split(":")
                    fundName=fund[1]
                    print("len=0",fundName)
                    break
            print(fundName)
        except Exception as e:
            print("in identify fund",e)
        return fundName