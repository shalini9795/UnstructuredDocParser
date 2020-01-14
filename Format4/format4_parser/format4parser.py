from datetime import datetime

import re
import pandas as pd
from config.config import CONFIG
import logging
from utils.utility import Utils
class Format4Parser:


    @staticmethod
    def concat_rowData(df):
        df = df.reset_index(drop=True)
        r = len(df.index) - 1
        #print(r)
        while r >= 0:
            temp_index = list()
            while (df.at[r, "Proposal No"] == None and df.at[r, "PROPOSED BY"] == None):
                temp_index.append(r)
                r -= 1
            # print(temp_index)
            r -= 1
            if len(temp_index) >= 1:
                c = temp_index[-1]
                # print(c)
                for m in reversed(temp_index):
                    df.at[c - 1, "PROPOSAL"] += " " + df.at[m, "PROPOSAL"]
                    df.at[m, "PROPOSAL"] = None

        df = df.dropna(subset=["PROPOSAL"])
        return df



    @staticmethod
    def identify_header(list):
        header_list = ['PROPOSED BY']
        temp = False
        for line in list:
            for str in header_list:
                if line.strip().find(str) > -1:
                    temp = True
                    break
            if temp == True:
                break
        # print(temp)
        return temp


    @staticmethod
    def companyfundDetails(content, fName):
        logger = Utils.add_logger()
        final_df = pd.DataFrame()
        companylist = content.split('---------------------------')
        for list in companylist:
            dlist = list.replace('PROPOSAL:', '##### \n PROPOSAL:', 1)
            dlist = dlist.replace(':\n', ':')
            dlist = dlist.replace('PROPOSAL\n', 'PROPOSAL ')
            klist = dlist.split('#####')
            #print(len(klist))
            if len(klist) > 1:
                dpd = Format4Parser.proposalData(klist[1])
                dcla = Format4Parser.companyData(klist[0])
                dpd['ID'] = 1
                dcla['ID'] = 1
                #print('--------------- !')
                try:
                    df_all_rows = pd.merge(dcla, dpd, on=['ID'])
                    df_all_rows['FundName'] = fName
                    final_df = final_df.append(df_all_rows)
                except Exception as error:
                    logger.error("Exception in merging companyfunddetails-{}".format(error))
            else:
                logger.debug("empty list in companydetails")
        return  final_df

    @staticmethod
    def proposal_identifier(list):
        list = list.replace('PROPOSAL #', '')
        list = list.replace("PROPOSAL:", "PROPOSAL")
        list = re.sub(':', '\xa0', list, 1)
        return list

    @staticmethod
    def proposalData(str):
        pattern = r'([a-z0-9]\:)'
        logger = Utils.add_logger()
        temp = False
        header = 0
        length = 0
        plist = str.split('\n')
        prev_list = []
        for list in plist:
            list = re.sub('\xa0+', '\xa0', list)
            if 'PROPOSAL' in list:
                list = Format4Parser.proposal_identifier(list)
            finallist = list.split('\xa0')
            #print(finallist)
            for index , value in enumerate(finallist):
                if value == "" or value ==' ':
                    finallist.pop(index)
            if len(prev_list)>=1:
                for values in finallist:
                    prev_list.append(values)
                prev_list[1]+= " " + prev_list.pop(2)
                finallist = prev_list
                prev_list = []
            for i in range(0, len(finallist)):
                finallist[i] = finallist[i].strip()
            #print(finallist)
            try:
                if temp == False:
                    temp = Format4Parser.identify_header(finallist)
                if temp == True and header == 0:
                    collist = []
                    for i in range(0,len(finallist)):
                        finallist[i] = finallist[i].strip()
                    collist.append("Proposal No")
                    for val in finallist:
                        collist.append(val)
                    df = pd.DataFrame(columns=collist)
                    col = collist
                    header = 1
                    length = len(collist)
                elif length == len(finallist) and temp == True:
                    df = df.append(pd.Series(finallist, index=col), ignore_index=True)
                    prev_list = []
                elif len(finallist) == 1 and temp == True and finallist[0]!="":
                    #print(finallist)
                    fin = finallist[0].strip()
                    finallist = []
                    finallist.append(None)
                    finallist.append(fin)
                    for i in range(len(finallist),length):
                        finallist.append(None)

                    #print(finallist)
                    df = df.append(pd.Series(finallist, index=col), ignore_index=True)
                elif len(finallist) == 2:
                    prev_list = finallist
                elif len(finallist) == 7:
                    finallist[1] += " " + finallist.pop(2)
                    df = df.append(pd.Series(finallist, index=col), ignore_index=True)
                else:
                    prev_list = []
            except Exception as error:
                logger.error("Exception in proposalData-{}".format(error))
                print("Exception in proposalData-{}".format(error))

        if temp == False:
            df = pd.DataFrame()
        return df


    @staticmethod
    def companyData(str):
        logger=Utils.add_logger()
        str = re.sub('\xa0+', '|', str)
        str = re.sub('\n+', ' ', str)
        str = str.replace(':|', ':')
        list = str.split('|')
        header = ['Header', 'Value']
        df = pd.DataFrame(columns=header)
        try:
            for col in list:
                col = col.replace(':', '##', 1)
                collist = col.split('##')
                if len(collist) > 1:
                    for index, value in enumerate(collist):
                        collist[index] = collist[index].strip()
                    df = df.append(pd.Series(collist, index=header), ignore_index=True)
        except Exception as error:
            logger.error("Exception in companyData-{}".format(error))
            print("Exception in companyData-{}".format(error))
        # print(df)
        dt = df.set_index('Header').transpose()
        return dt

    @staticmethod
    def fundName(str):
        str = re.sub('\xa0+', '', str)
        str = re.sub('\n+', ' ', str)
        str = re.sub('\-{2,}', '', str)
        str = str.split(':')
        return str[1].strip()
