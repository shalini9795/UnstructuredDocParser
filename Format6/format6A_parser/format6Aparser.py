import pandas as pd
import re
from utils.utility import Utils
class Format6parser:


    @staticmethod
    def segparsing(contents):
        match1 = contents.upper().rfind('<html>')
        mat = len(contents)  # contents.upper().find('SIGNATURES')
        if mat == -1:
            mat = len(contents)
        if match1 > -1:
            report8k = contents[match1:mat]
        else:
            match1 = contents.upper().find('</html>')
            if match1 > -1:
                report8k = contents[match1:mat]
            else:
                report8k = contents
        return report8k

    @staticmethod
    def findHeader(listitem):
        if listitem.count('Security') > 0:
            return True

    @staticmethod
    def findDirector(listitem):
        director_pattern1 = 'The election of two Class II directors'
        direct_pattern2 = 'The election of three Class III directors'
        if listitem.count('Class III Nominees:') > 0 or listitem.count('Election of Directors:') > 0 or listitem.count('DIRECTOR') > 0  or listitem.count('Election of Directors')>0 \
                or listitem.count('DIRECTORS') > 0\
                or len([i for i in listitem if director_pattern1 in str(i) or direct_pattern2 in str(i)])>=1:
            return True
        else:
            return False

    @staticmethod
    def findfooter(listitem):
        if listitem.count('Item') > 0:
            return True

    @staticmethod
    def findAccout(listitem):
        if listitem.count('Account Name') > 0 or listitem.count('Custodian') > 0:
            return True

    @staticmethod
    def splitItemValue(header, footer, line):
        lstitem = []
        if header == 0 and footer == 0:
            lstitem = line
        elif header == 1 and footer == 0:
            print('header')
            #print(line)
            #print(lstitem)
        elif header == 0 and footer == 1:
            print('footer')
            #print(line)
            lstitem = line

    @staticmethod
    def identify_header(list):
        header_list = ['Item', 'Proposal', 'For/Against Management']
        temp = False
        for str in header_list:
            for line in list:
                if line.upper().find(str.upper()) > -1:
                    temp = True
                    break
        return temp

    @staticmethod
    def formDataFrame(dfa):
        droplist = []
        # print(dfa)
        for col in dfa.columns:
            if col.lower().find('companyname') > -1:
                dfa.rename(columns={col: 'CompanyName'}, inplace=True)
            elif col.lower().find('proposedby') > -1 or col.lower().find('proposed by') > -1 or col.lower()=='type':
                dfa.rename(columns={col: 'Proposedby'}, inplace=True)
            elif col.lower().find('meeting date') > -1:
                dfa.rename(columns={col: 'MeetingDate'}, inplace=True)
            elif col.lower().find('meeting type') > -1:
                dfa.rename(columns={col: 'MeetingType'}, inplace=True)
            elif col.lower().find('ticker symbol') > -1:
                dfa.rename(columns={col: 'SymbolType'}, inplace=True)
            elif col.lower().find('proposal') > -1:
                dfa.rename(columns={col: 'Proposal'}, inplace=True)
            elif col.lower().find('isin') > -1:
                dfa.rename(columns={col: 'ISIN'}, inplace=True)
            # elif col.lower().find('record date')>-1:
            #    dfa.rename(columns={col:'RecordDate'},inplace=True)
            elif col.lower().find('security') > -1:
                dfa.rename(columns={col: 'Security'}, inplace=True)
            elif col.lower().find('for/againstmanagement') > -1 or col.lower().find('for/against') > -1:
                dfa.rename(columns={col: 'For/Against'}, inplace=True)
            elif col.lower().find('management recommendation') > -1 \
                    or col.lower().find('ManagementRecommendation') > -1 or col.lower().find('mgt. rec') > -1:
                dfa.rename(columns={col: 'MangementRecommendation'}, inplace=True)
            elif col.lower().find('item') > -1:
                dfa.rename(columns={col: 'ProposalNo'}, inplace=True)
            elif col.lower().find('vote') > -1:
                dfa.rename(columns={col: 'Vote'}, inplace=True)
            else:
                droplist.append(col)

        if dfa.empty:
            #print('DataFrame is empty!')
            dfa = dfa.drop(columns=droplist)
        else:
            #print(droplist)
            dfa = dfa.drop(columns=droplist)
        return dfa

    @staticmethod
    def listline(item, length, col, footer, listline):
        itemlen = len(item)
        if item.count('CMMT') > 0 and footer == 1 and length != itemlen:
            size = length - len(item)
            item.extend([''] * size)
        elif (item.count('Management') > 0 or item.count(
                'For') > 0 or item.count('Non-Voting')) and footer == 1 and length != itemlen and itemlen > 1 and Format6parser.findDirector(item) == False and Format6parser.findDirector(listline) == False:
            size = length - len(item)
            item.extend([''] * size)
        return item

    @staticmethod
    def finalOutput(headeritem, companyName, df):
        dfs = Format6parser.splitevenodd(headeritem, companyName)
        df_all_rows = Format6parser.combineDataSet(df, dfs)
        df_all = Format6parser.formDataFrame(df_all_rows)
        # print(df_all)
        return df_all

    @staticmethod
    def combineDataSet(df, dfs):
        df['ID'] = 1
        dfs['ID'] = 1
        # print(df)
        # print(dfs)
        df_all_rows = pd.merge(df, dfs, on=['ID'])
        # print(df_all_rows)
        return df_all_rows

    @staticmethod
    def splitevenodd(headeritem, companyName):
        logger = Utils.add_logger()
        colist = []
        headerlist = []
        headeritem = [x for x in headeritem if x not in ['Management', 'Opposition']]
        # print("headeritem:{}".format(headeritem))
        length = len(headeritem)
        try:
            for i in range(0, length + 1):
                # print(length)
                if (i % 2 == 0):
                    headerlist.append(headeritem[i])
                else:
                    if "Date" in headeritem[i] or "Agenda" in headeritem[i]:
                        headeritem.insert(i, " ")
                        colist.append("  ")
                        length += 1
                    else:
                        colist.append(headeritem[i])
        except:
            logger.debug("colList:{}".format(colist))
            #print("colList:{}".format(colist))
        dfs = pd.DataFrame(columns=headerlist)
        # print("headeritem after loop:{}".format(headerlist))
        # print("collist:{}".format(colist))
        dfs = dfs.append(pd.Series(colist, index=headerlist), ignore_index=True)
        #print("CompanyName={}".format(companyName))
        if len(companyName) >= 1:
            dfs['CompanyName'] = companyName
        #print(dfs)
        return dfs

    @staticmethod
    def headlerlistcreation(headeritem):
        logger = Utils.add_logger()
        colist = []
        headerlist = []
        headeritem = [x for x in headeritem if x not in ['Management', 'Opposition']]
        # print("headeritem:{}".format(headeritem))
        length = len(headeritem)
        try:
            for i in range(0, length + 1):
                if (i % 2 == 0):
                    headerlist.append(headeritem[i])
                else:
                    if "Date" in headeritem[i] or "Agenda" in headeritem[i]:
                        headeritem.insert(i, " ")
                        colist.append("  ")
                        length += 1
                    else:
                        colist.append(headeritem[i])
        except:
            logger.debug("colList:{}".format(colist))
            #print("colList:{}".format(colist))
        #print("headeritem after loop:{}".format(headerlist))
        #print("collist:{}".format(colist))
        alist = []
        for i, j in zip(headerlist, colist):
            alist.append(i)
            alist.append(j)
        #print(alist)
        return alist

    @staticmethod
    def addManagement(listActua, listlastItem):
        finallist = []
        i = 0
        str = ''
        for value in listActua:
            if i < len(listlastItem):
                str = listlastItem[i]
                i = i + 1
            if i <= 2:
                finallist.append(str + ' ' + value)
            else:
                finallist.append(value)
                if finallist.count(str) == 0:
                    finallist.insert(2, str)
        return finallist

    @staticmethod
    def addLstItemValue(listActua, listlastItem):
        i = 0
        finallist = []
        for value in listlastItem:
            text = ''
            if len(listActua) == 1:
                if i == 0:
                    value = listlastItem[i] + str(i+1)
                elif i == 1:
                    text = listActua[i-1]
            elif i < len(listActua):
                text = listActua[i]
            finallist.append(value + ' ' + text)
            i += 1
        return finallist

    @staticmethod
    def tabledetails(table):
        line = []
        dffinal = pd.DataFrame()
        header = 0
        headeritem = []
        footer = 0
        account = 0
        lstitem = []
        hlist=[]
        ditem = []
        ftext = ''
        df = None
        temp = 0
        m = 1
        companyName = ''
        col = []
        length = 0
        lenrow = 0
        # df=pd.DataFrame()
        table_rows = table.find_all('tr')
        tblerowlen = len(table_rows)
        for idx, tr in enumerate(table_rows):
            lenrow = lenrow + 1
            td = tr.find_all('td')
            for i in td:
                if i.text.strip() != '' or i.text.strip() == '':
                    ftext = i.text.strip().replace('\n', ' ').replace('\xa0', ' ')
                    ftext = re.sub(' +', ' ', ftext)
                    line.append(ftext)
                    ftext = ''
            #line = [x for x in line if x!= ' ' or x!= '']
            hlist=line
            line = list(filter(None, line))
            if line!=None and len(line)>0 and line!=[]:
                if footer==1:
                    line=Format6parser.listline(line,length,col,footer,lstitem)

                if Format6parser.findHeader(line) or idx + 1 == len(table_rows):
                    m = 1
                    if df is not None:
                        if len(line) == length and account == 0:
                            df = df.append(pd.Series(line, index=col), ignore_index=True)

                        df_all = Format6parser.finalOutput(headeritem, companyName, df)
                        #print(df_all)
                        dffinal = dffinal.append(df_all, sort=True)
                        headeritem = []
                        companyName = ''

                if Format6parser.findHeader(line):
                    header = 1
                    footer = 0
                    account = 0
                    temp = False
                    companyName = ditem
                elif Format6parser.findfooter(line):
                    footer = 1
                    header = 0
                    account = 0
                elif Format6parser.findAccout(line):
                    footer = 0
                    header = 0
                    account = 1

                # print(header)
                # print(footer)
                # print(account)

                if header == 0 and footer == 0 and account == 0:
                    lstitem = line
                    # ditem=line
                    temp = False
                elif header == 1 and footer == 0 and account == 0:
                    if m <= 4:
                        headeritem = headeritem + Format6parser.headlerlistcreation(line)
                        m = m + 1
                    # companyName=lstitem
                elif header == 0 and footer == 1 and account == 0:
                    if temp == False:
                        temp = Format6parser.identify_header(line)
                        if temp == True:
                            df = pd.DataFrame(columns=line)
                            col = line
                            length = len(line)
                            lstitem = line
                            # ditem=line
                        # print(line)
                    elif tblerowlen == lenrow:
                        lstitem = line
                        # ditem=line
                    else:
                        # print(ditem)
                        if len(line) != length and len(lstitem) == length and Format6parser.findDirector(lstitem):
                            ddata = []
                            ddata = Format6parser.addLstItemValue(line, lstitem)
                            if len(ddata) == length:
                                df = df.append(pd.Series(ddata, index=col), ignore_index=True)
                            else:
                                print(ddata)
                        elif len(line) != length and Format6parser.findDirector(lstitem):
                            directdata = []
                            directdata = Format6parser.addManagement(line, lstitem)
                            if len(directdata) == length:
                                df = df.append(pd.Series(directdata, index=col), ignore_index=True)
                            else:
                                print(directdata)
                        elif len(line) == length and Format6parser.findDirector(line)==False:
                            df = df.append(pd.Series(line, index=col), ignore_index=True)
                            lstitem = line
                                # ditem=line
                        else:
                            # print('--------------')
                            lstitem = line
                            # ditem=line

                elif header == 0 and footer == 0 and account == 1 and Format6parser.findAccout(line):
                    print('Excluded')
                    # m=1
                    # print(headeritem)
                    # df_all=finalOutput(headeritem,companyName,df)
                    # dffinal=dffinal.append(df_all,sort=True)
                    # print(companyName)
                    # print(headeritem)
                    # headeritem=[]
                    # companyName=''

                else:
                    lstitem = line

                ditem = line
                line = []
            else:
                if idx + 1 == len(table_rows):
                    if df is not None:
                        df_all = Format6parser.finalOutput(headeritem, companyName, df)
                        #print(df_all)
                        dffinal = dffinal.append(df_all, sort=True)
                        headeritem = []
                        companyName = ''
                line = []

        # print(dffinal)
        return dffinal


