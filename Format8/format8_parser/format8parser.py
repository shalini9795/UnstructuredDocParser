from bs4 import BeautifulSoup
import re
import pandas as pd
from utils.utility import Utils
class Format8Parser:



    @staticmethod
    def identify_header(list, accession_no):
        header_list = ['Country','CUSIP','Symbol','Meeting Type','Ticker','Name of Issuer','For/Against Management']
        temp = False
        for text in header_list:
            for line in list:
                if line.upper().find(text.upper()) > -1:
                    temp = True
                    with open('C:\\Users\\rmore\\Documents\\equity-personneldb-npxfiling\\data\\format8_headerlist.txt','a+') as file:
                        file.write((accession_no) + ":" + str(list))
                        file.write('\n')
                    return temp
        return temp

    @staticmethod
    def segparsing(contents):
        match1 = contents.upper().find('VOTING RECORD')
        #mat = contents.upper().find('SIGNATURES')
        mat = -1
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
    def tabledetails(table, lstdf, accession_no):
        logger = Utils.add_logger()
        line = []
        temp = False
        length = 0
        head = 0
        tempf = 0
        fund = ''
        parseline = []
        checkdata = []
        table_rows = table.find_all('tr')
        for tr in table_rows:
            try:
                td = tr.find_all('td')
                for i in td:
                    line.append(i.text.replace('\n', '').replace('\xa0', ' '))
                    checkdata.append(i.text.strip().replace('\n', '').replace('\xa0', ' '))
                line = [(lambda x: x.strip())(l) for l in line if l!=' ']
                #print(line)
                # if "QSR" in line or  "FP" in line:
                #     print("Line=={}".format(line))
                if line == [] or len(line) == line.count(''):
                    line = [];
                    continue;
                if temp == False:
                    temp = Format8Parser.identify_header(line, accession_no)
                    head = head + 1
                    # print('head ' + str(head))
                    # print('temp ' + str(temp))
                    # print('temp ' + str(len(line)))
                    # print(len(lstdf))
                    if temp == False and head == 1 and len(lstdf) != 0:
                        temp = True
                        parseline = line
                        if len(lstdf) == len(line):
                            line = lstdf
                        else:
                            lstdf = list(lstdf)
                            abcd = set(lstdf)
                            if 'Fund Name' in abcd:
                                lstdf.remove('Fund Name')
                            if 'FundName' in abcd:
                                lstdf.remove('FundName')
                            line = lstdf
                    if temp == True:
                        line = [(lambda x: re.sub(' +', ' ', x))(l) for l in line if l != ' ']
                        df = pd.DataFrame(columns=line)
                        col = line
                        length = len(line)
                        if parseline != [] and len(parseline) == len(line):
                            df = df.append(pd.Series(parseline, index=col), ignore_index=True)
                        #print("header")
                        #print(line)
                        #print(length)
                        ##print(df)
                elif length == len(line):
                    df = df.append(pd.Series(line, index=col), ignore_index=True)
                elif length==len(checkdata):
                    df = df.append(pd.Series(checkdata, index=col), ignore_index=True)
                line = []
                checkdata=[]
            except Exception as error:
                logger.error("Exception in format8 parser for accession_no:{} -{}".format(accession_no,error))
                print("Exception in format8 parser:{}".format(error))
        if temp == False:
            df = pd.DataFrame()
        else:
            for col in df.columns:
                if col.upper().find('FUND NAME') > -1:
                    tempf = 1
            if tempf != 1 and fund != '':
                df["Fund Name"] = fund
        return df


    @staticmethod
    def findfundName(content):
        # print(content)
        fundname = ''
        i = 0
        fundVoteData = content.split('@@@@@@@@@@@@@@@@@@@@@')
        content = fundVoteData[0]
        # print(content)
        soup = BeautifulSoup(content, "html.parser")
        contents = soup.text
        # print(contents)
        contents = re.sub('\xa0+', ' ', contents)
        contents = re.sub('\n+', '\n', contents)
        alist = contents.split('\n')
        clen = len(alist)
        for klist in alist[::-1]:
            #print([klist])
            if klist.strip() != '':
                i = i + 1
                if i <= 2:
                    fundname = klist + ' ' + fundname.strip()
        return fundname


    @staticmethod
    def tableparsed(content, olderFundName, lstdf, accession_no):
        temp = 0
        soup = BeautifulSoup(content, "html.parser")
        fundname = ''
        column = ''
        tables = soup.findAll('table')
        for table in tables:
            dfa = Format8Parser.tabledetails(table, lstdf, accession_no)
            if dfa.empty == False:
                for col in dfa.columns:
                    column = col.upper().strip()
                    if column.strip().find('FUND NAME') > -1 or column.strip().find('FUNDNAME') > -1:
                        temp = 1
                if temp == 1:
                    return dfa
                else:
                    fundname = Format8Parser.findfundName(content)
                    if fundname == '':
                        fundname = olderFundName
                    dfa["Fund Name"] = fundname
                    return dfa
            if dfa.empty == True:
                print('')
    @staticmethod
    def remove_spaces_from_df(df):
        convert_dict = {'FundName': str,
                        'CompanyName': str,
                        'Proposal': str}
        #df = df.astype(convert_dict)
        col_name = df.columns
        if 'FundName' in col_name and 'CompanyName' in col_name and 'Proposal' in col_name:
            df = df.astype(convert_dict)
            df['FundName'] = df['FundName'].apply(Utils.remove_spaces)
            df['CompanyName'] = df['CompanyName'].apply(Utils.remove_spaces)
            df['Proposal'] = df['Proposal'].apply(Utils.remove_spaces)
        return df


    @staticmethod
    def formDataFrame(dfa):
        droplist = []
        #print("Columns before renaming:{}".format(dfa.columns))
        for col in dfa.columns:
            if col.lower().find('proposed') > -1 \
                    or col.lower().find('proponent') > -1\
                    or col.lower().find('was this a shareholder or mgmt proposal?') > -1:
                dfa.rename(columns={col: 'ProposedBy'}, inplace=True)
            elif col.lower().find('issuer') > -1 or col.lower().find('company name') > -1\
                    or col.lower().find('security name') > -1 or col.lower().find('company') > -1:
                dfa.rename(columns={col: 'CompanyName'}, inplace=True)
            elif col.lower().find('ticker symbol') > -1 or col.lower().find('exchange ticker symbol') > -1\
                    or col.lower().find('ticker') > -1:
                dfa.rename(columns={col: 'Ticker'}, inplace=True)
            elif col.lower().find('country') > -1:
                dfa.rename(columns={col: 'Country'}, inplace=True)
            elif col.lower().find('meeting date') > -1 or col.lower().find('shareholder meeting date ') > -1\
                    or col.lower().find('meetingdate') > -1:
                dfa.rename(columns={col: 'MeetingDate'}, inplace=True)
            elif col.lower().find('meeting type') > -1:
                dfa.rename(columns={col: 'MeetingType'}, inplace=True)
            elif col.lower().find('security id') > -1 or col.lower().find('cusip') > -1:
                dfa.rename(columns={col: 'SecurityID'}, inplace=True)
            elif col.lower().find('symbol') > -1:
                dfa.rename(columns={col: 'SymbolType'}, inplace=True)
            elif col.lower().find('proposal text') > -1 or col.lower().find('summary of matter voted on') > -1\
                    or col.lower().find('brief identification of the matter voted On') > -1\
                    or col.lower().find('brief description of matter') > -1\
                    or col.lower().find('description of vote') > -1 or col.lower().find('summary') > -1\
                    or col.lower().find('issue') > -1 or col.lower().find('proposal') > -1:
                dfa.rename(columns={col: 'Proposal'}, inplace=True)
            elif col.lower().find('mgmt reco') > -1:
                dfa.rename(columns={col: 'MangementRecommendation'}, inplace=True)
            elif col.lower().find('vote cast') > -1 \
                    or col.lower().find('fund’s vote for or against proposal, or abstain; for or withhold regarding election of directors') > -1 \
                    or col.lower().find('how did the fund cast its vote? for, against, abstain') > -1\
                    or col.lower().find('how voted') > -1 or col.lower().find('abstain') > -1\
                    or col.lower().find('votecast') > -1:
                dfa.rename(columns={col: 'VoteCast'}, inplace=True)
            elif col.lower().find('fund name') > -1 or col.lower().find('fundname') > -1:
                dfa.rename(columns={col: 'FundName'}, inplace=True)
            elif col.lower().find('logical ballot status') > -1 or col.lower().find('ballot status') > -1\
                    or col.lower().find('whether fund cast vote on matter') > -1 \
                    or col.lower().find('did the fund vote?') > -1\
                    or col.lower().find('mattervoted') > -1\
                    or col.lower().find('cast') > -1:
                dfa.rename(columns={col: 'WhetherVotedorNot'}, inplace=True)
            elif col.lower().find('agenda item #') > -1 or col.lower().find('proposal number') > -1:
                dfa.rename(columns={col: 'ProposalNumber'}, inplace=True)
            elif col.lower().find('whether vote was for or against management') > -1 \
                    or col.lower().find('did the fund vote For or against management?') > -1\
                    or col.lower().find('for /againstmgmt') > -1 or col.lower().find('with management') > -1\
                    or col.lower().find('mgmt reco') > -1\
                    or col.lower().find('voted against   mgmt yes/no') > -1:
                dfa.rename(columns={col: 'For/AgainstManagement'}, inplace=True)
            elif col.lower() == 'voted':
                dfa.rename(columns={col: 'Voted'}, inplace = True)
            elif col.lower().find('vote instruction') > -1\
                    or col.lower().find('vote   instruction') > -1:
                dfa.rename(columns = {col : 'VoteInstruction'}, inplace = True)
            else:
                droplist.append(col)
        #droplist = [i for i in dfa.columns if i == 'Record Date' or i == '']
        dfa = dfa.drop(columns = droplist)
        #print("column name after rename:{}".format(dfa.columns))
        return dfa

