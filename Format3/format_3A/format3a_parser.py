from bs4 import BeautifulSoup
import glob
import os
import re
import contextlib
import pandas as pd
import logging.config

from Format3.format_3A.config.config import CONFIG
from utils.utility import Utils


class Format3AParser:
    sreqData = ''

    regexStartSegment = r"pre>"
    regexEndSegment = r"signature"
    pattern_startSeg = re.compile(regexStartSegment, re.MULTILINE | re.IGNORECASE)
    patter_endSeg = re.compile(regexEndSegment, re.MULTILINE | re.IGNORECASE)

    ##FUNCTIONS file reader
    @staticmethod
    def read_file(accession_no):
        try:
            path = CONFIG["Path"]["path"] + accession_no + ".dissem"
            f = open(path, 'r')
        except IOError as e:
            print('File not found')
        except:
            try:
                f = open(path, 'r', encoding="utf-8")
                contents = f.read()
            except:
                print('File error')
        return contents

    # -----------------------Segment Selector-------------------------------#
    @staticmethod
    def segment_selector(contents):
        sreqData = ''
        bsoupraw = BeautifulSoup(contents, "html.parser")
        reqData = bsoupraw.find('pre')
        if reqData is None:
            print('!------Segment finding keyword not found------!')
        else:
            sreqData = reqData.getText()
        return sreqData

    # -----------------------Data Cleaner & formatter-------------------------------#
    @staticmethod
    def data_cleaner(sreqData):
        cleanreqData = re.sub(r'\n{2,}', '\n', sreqData)
        listreqData = re.split(r'_{4,}', cleanreqData)
        return listreqData

    # ------------------------------Data Extractor-----------------------------------------------#
    @staticmethod
    def data_extractor(listreqData):
        fund_name = ''
        companyname = ''
        ticker = ''
        secuid = ''
        meetdate = ''
        meetstatus = ''
        meettype = ''
        countryoftrade = ''
        issue_num = ''
        description = ''
        proponent = ''
        mgmtrec = ''
        votecast = ''
        Fr_Agnst_mgmt = ''
        headerdata_list = []
        whole_data = []
        df = pd.DataFrame(
            columns=['SeqId', 'FundName', 'CompanyName', 'Ticker', 'SecurityID', 'MeetingDate', 'MeetingStatus',
                     'MeetingType', 'CountryofTrade', 'IssueNo', 'Description', 'Proponent', 'MgmtRec', 'VoteCast'
                                                                                                        'For/Against Mgmt'])
        seq_id = 1

        for delimiteddata in listreqData:  # 0:1000 , 515:1000(has fund name)
            # print(data)
            if re.search(r"\n{2}([a-z].*)\n{2}", delimiteddata,
                         flags=re.MULTILINE | re.IGNORECASE):  # regex for fundname \n\nfname\n\n
                # print('------------>', delimiteddata)
                # print('-->',type(delimiteddata))
                # print('1) Original Table Structure With Fund Name--> \n\n', delimiteddata, '\n-----------------------------------\n')
                # delimiteddata
                newline_delimitedlist = delimiteddata.split('\n')
                # print('2) List of data in Table having Fund Name split by newline----> \n\n', newline_delimitedlist, '\n========================================\n')
                # for i, data in enumerate(newline_delimitedlist):
                # print('count - ', i+1)
                # print('Length : ', len(newline_delimitedlist))
                l = len(newline_delimitedlist)
                print('\n*******************************\n', 'Fund Name : ', newline_delimitedlist[l - 3].strip(),
                      '\n*******************************\n')  # sks
                fund_name = newline_delimitedlist[l - 3].strip()
            #         headerdata_list.append(fund_name)
            #         print('*---->', headerdata_list)
            # extracteddata.update({'Fund Name': fund_name})

            elif len(delimiteddata) > 0:
                newline_delimitedlist = delimiteddata.split('\n')
                for i, data in enumerate(newline_delimitedlist):
                    if re.search(r'ticker', data, flags=re.MULTILINE | re.IGNORECASE):
                        ####This part extracts the comapny name on the basis of index of ticker
                        cname = newline_delimitedlist[i - 1]
                        # print('-----------ticker_data>', data)
                        # print('-----------Companyname>', cname.strip())
                        companyname = cname.strip()
                        # extracteddata.update({'Fund Name': fund_name})
                        # extracteddata.update({"Company name" : companyname})
                        header1data = newline_delimitedlist[i + 1]
                        cleanreqData = re.sub(r'\s+', '\t', header1data)
                        v = cleanreqData.split('\t')
                        #                 print('val--', v)
                        #                 print(type(v))

                        ticker = v[0].strip()
                        try:
                            v2 = v[2].strip()
                        except IndexError:
                            v2 = ''
                        secuid = v[1].strip() + ' ' + v[2].strip()
                        meetdate = v[3].strip()
                        meetstatus = v[4].strip()
                        # print('Fundname :' ,fund_name)
                        print('----------------------------------', 'Header Data : ',
                              '-----------------------------------')
                        print('CompanyName : ', companyname)
                        print('Ticker : ', ticker)
                        #                 headerdata_list.append(ticker)
                        print('Security ID:', secuid)
                        #                 headerdata_list.append(secuid)
                        print('Meeting Date :', meetdate)
                        #                 headerdata_list.append(meetdate)
                        print('Meeting Status : ', meetstatus)
                    #                 headerdata_list.append(meetstatus)
                    #                 print('*---->', headerdata_list)

                    elif re.search(r'meeting type', data, flags=re.MULTILINE | re.IGNORECASE):
                        header2data = newline_delimitedlist[i + 1]
                        cleanreqData = re.sub(r'\s+', '\t', header2data)
                        v = cleanreqData.split('\t')
                        # print('---->',v)
                        meettype = v[0].strip()
                        try:
                            v2 = v[2].strip()
                        except IndexError:
                            v2 = ''
                        countryoftrade = v[1].strip() + ' ' + v2
                        print('Meeting Type : ', meettype)
                        print('Country of Trade : ', countryoftrade)
                    #                 headerdata_list.append(seq_id)
                    #                 headerdata_list.append(fund_name)
                    #                 headerdata_list.append(ticker)
                    #                 headerdata_list.append(secuid)
                    #                 headerdata_list.append(meetdate)
                    #                 headerdata_list.append(meetstatus)
                    #                 headerdata_list.append(meettype)
                    #                 headerdata_list.append(countryoftrade)
                    #                 seq_id +=1

                    #                 print('*---->', headerdata_list)
                    #                 #insert into df row
                    #                 headerdata_list = []

                    #                 print('------------------------------------------------------------------------------------------')

                    ###Table parser elif
                    elif re.search(r'issue no', data, flags=re.MULTILINE | re.IGNORECASE):
                        # print('--------->', newline_delimitedlist[i+3:])
                        tabledata = newline_delimitedlist[i + 2:]
                        issue_num = []
                        description = []
                        proponent = []
                        mgmtrec = []
                        votecast = []
                        Fr_Agnst_mgmt = []
                        for i, rowdata in enumerate(tabledata):
                            #                     print('column :', rowdata)
                            data = re.split(r"\s{2,}", rowdata)
                            #                     print('element data :', len(data))

                            if len(data) == 6:
                                #                         print('yes')
                                issue_num.append(data[0].strip())
                                description.append(data[1].strip())
                                proponent.append(data[2].strip())
                                mgmtrec.append(data[3].strip())
                                votecast.append(data[4].strip())
                                Fr_Agnst_mgmt.append(data[5].strip())


                            elif len(data) < 6:

                                #                         print('no')
                                descr = description.pop()
                                #                         print('**************>', descr)
                                description.append(descr + " ".join(data))
                        #                         print('------------->', description)
                        z = list(zip(issue_num, description, proponent, mgmtrec, votecast, Fr_Agnst_mgmt))

                        print('=============Table Data=======================')
                        print('TableData Len: ', len(z), '| TableData : ', z)
                        print('==============================================')

                        headerdata_list.append(seq_id)  # sks
                        headerdata_list.append(fund_name)  # sks
                        headerdata_list.append(companyname)
                        headerdata_list.append(ticker)  # sks
                        headerdata_list.append(secuid)  # sks
                        headerdata_list.append(meetdate)  # sks
                        headerdata_list.append(meetstatus)  # sks
                        headerdata_list.append(meettype)  # sks
                        headerdata_list.append(countryoftrade)  # sks
                        seq_id += 1  # sks

                        #                 print('*headerlist---->', headerdata_list, '<----*') #sks

                        for i in z:
                            # print('----->', headerdata_list + list(i))
                            whole_data.append(headerdata_list + list(i))

                        headerdata_list = []
        #                 print('-------------->', whole_data)
        # df = pd.DataFrame(whole_data, columns=['SeqId','FundName','CompanyName','Ticker','SecurityID','MeetingDate','MeetingStatus','MeetingType','CountryofTrade','IssueNo','Description','Proponent','MgmtRec','VoteCast'
        #     'For/Against Mgmt'])
        df = pd.DataFrame(whole_data)
        return df
