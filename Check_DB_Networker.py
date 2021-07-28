#!/usr/bin/python3
### Check_DB_Networker_v27.py(Check_DB_Networker_v7.py) - Paragraph <3>+<5> in the technical assignment   ###
from __future__ import print_function
import os
import platform
import sys
import subprocess
import datetime
from contextlib import redirect_stdout

if platform.system() == 'Windows':
    TEST_MODE = True
else:
    TEST_MODE = False

##############################################################################################################################
###              Check_DB_Backup_v11.py - Paragraph 3 in the technical assignment                                          ###
##############################################################################################################################

TIME_SHIFT = 15
DEPH1 = 12
DataToday = str(datetime.date.today())
ThisYear = DataToday[:4]
DataYesterday = str(datetime.date.today() - datetime.timedelta(days=1))
CurHour = datetime.datetime.now().hour
LastYear = str(datetime.date.today() - datetime.timedelta(days=365))[:4]

if TEST_MODE:
    DIR_init_db_log = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\log\\'
    FLAG_DWH = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\flag_DWH_backup.flg'
    FLAG_MART = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\flag_MART_backup.flg'
    FLAG_DHUB = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\flag_DATAHUB_backup.flg'
    SummaryLog_Name = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\DB_BAckup.log'
    CheckDBbckp_log = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\check_DB_backup.txt'
else:
    DIR_init_db_log = '/db2home/scripts/init_db/log/'
    FLAG_DWH = '/tmp/flag_DWH_backup.flg'
    FLAG_MART = '/tmp/flag_MART_backup.flg'
    FLAG_DHUB = '/tmp/flag_DATAHUB_backup.flg'
    SummaryLog_Name = '/var/logs/DB_BAckup.log'
    CheckDBbckp_log = '/var/logs/check_DB_backup.txt'

NotStartedFlg = True
DWH_OK = 'Not_Started'
DHUB_OK = 'Not_Started'
MART_OK = 'Not_Started'

FILE_MART_LOG = ''
FILE_DWH_LOG = ''
FILE_DATAHUB_LOG = ''
#MESSAGE = ''
STR1 = []
ALL_DATA = set()
DWH_DBbckp_DICT = {}
DHUB_DBbckp_DICT = {}
MART_DBbckp_DICT = {}

if not TEST_MODE:
    subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)


def enumeratefiles(path):
    file_collection = []
    for dirpath, dirnames, filenames in os.walk(path):
        for file in filenames:
            file_collection.append(file)
    return file_collection


FILE_LOGS_NAME = enumeratefiles(DIR_init_db_log)
for i in FILE_LOGS_NAME:
    if ThisYear in i[:4] or LastYear in i[:4] :
        ALL_DATA.add(i[:10])
ALL_DATA = sorted(ALL_DATA, reverse=True)

# if CurHour > TIME_SHIFT:
#    if ALL_DATA[0] ==  DataToday :
#        ALL_DATA = ALL_DATA[1:]

f_SummLog = open(SummaryLog_Name, 'w')
f_SummLog.close()

count_data = 0
for DATA in ALL_DATA:
    count_data = count_data + 1
    DWH_OK = 'False'
    DHUB_OK = 'False'
    MART_OK = 'False'
    MART_SHIFT = 0
    DWH_SHIFT = 0
    DATAHUB_SHIFT = 0


    for i in FILE_LOGS_NAME:
        if DATA in i:
            if 'MART' in i:
                #if int(i[11:13]) >= TIME_SHIFT:
                #if  int(i[11:13]) > MART_SHIFT:
                    MART_SHIFT = int(i[11:13])
                    FILE_MART_LOG = i
            elif 'DWH' in i:
                #if int(i[11:13]) >= TIME_SHIFT:
                #if int(i[11:13]) > DWH_SHIFT:
                    DWH_SHIFT = int(i[11:13])
                    FILE_DWH_LOG = i
            elif 'DATAHUB' in i:
                #if int(i[11:13]) >= TIME_SHIFT:
                #if int(i[11:13]) > DATAHUB_SHIFT:
                    DATAHUB_SHIFT = int(i[11:13])
                    FILE_DATAHUB_LOG = i




    if FILE_MART_LOG == '':
        MART_OK = 'Not_Started'
    else:
        NotStartedFlg = True
        file = open(DIR_init_db_log + FILE_MART_LOG)
        inp = file.readlines()
        for ii in iter(inp):
            if "Backing up the 'MART' database" in ii:
                NotStartedFlg = False
            STR1.append(ii)
            if '+ exit 0' in ii:  # and 'NMDA backup was successful.' in STR1[-9]:
                for iii in range(DEPH1):
                    if 'NMDA backup was successful.' in STR1[-iii]:
                        MART_OK = 'True'

    if NotStartedFlg:
        MART_OK = 'Not Started'



    if FILE_DWH_LOG == '' :
        DWH_OK = 'Not_Started'
    else:
        NotStartedFlg = True
        file = open(DIR_init_db_log + FILE_DWH_LOG)
        inp = file.readlines()
        for ii in iter(inp):
            if "Backing up the 'DWH' database" in ii:
                NotStartedFlg = False
            STR1.append(ii)
            if '+ exit 0' in ii:  # and 'NMDA backup was successful.' in STR1[-9]:
                for iii in range(DEPH1):
                    if 'NMDA backup was successful.' in STR1[-iii]:
                        DWH_OK = 'True'

    if NotStartedFlg:
        DWH_OK = 'Not_Started'



    if FILE_DATAHUB_LOG == '':# or DATAHUB_SHIFT < TIME_SHIFT:
        #print('FAILED !!!.  DATAHUB - backup is NOT succeeded (not found the log of the corresponding backup) !!!')
        DHUB_OK = 'Not_Started'
    else:
        NotStartedFlg = True
        file = open(DIR_init_db_log + FILE_DATAHUB_LOG)
        inp = file.readlines()
        for ii in iter(inp):
            if "Backing up the 'DATAHUB' database" in ii:
                NotStartedFlg = False
            STR1.append(ii)
            if '+ exit 0' in ii:
                for iii in range(DEPH1):
                    if 'NMDA backup was successful.' in STR1[-iii]:
                        DHUB_OK = 'True'
    if NotStartedFlg:
        DHUB_OK = 'Not_Started'


    f_MART = open(FLAG_MART, 'w')
    f_DWH = open(FLAG_DWH, 'w')
    f_DHUB = open(FLAG_DHUB, 'w')
    f_SummLog = open(SummaryLog_Name, 'a')

    data_ind_mod = datetime.datetime(int(DATA[:4]), int(DATA[5:7]), int(DATA[8:10]))
    #print(str(DATA[:4])+str(DATA[5:7])+str(DATA[8:10]))
    PreDay = str((data_ind_mod - datetime.timedelta(days=1)).date())  # .replace('-', '')

    if MART_OK == 'True':
        if MART_SHIFT >= TIME_SHIFT :
            if count_data == 1:
                f_MART.write('1' + '\n')
            MART_DBbckp_DICT[DATA] =  ';MART;True'
            #f_SummLog.write(DATA + ';MART;True\n')
        else:
            if count_data == 1:
                f_MART.write('1' + '\n')
            MART_DBbckp_DICT[PreDay] = ';MART;True'
            #f_SummLog.write(PreDay + ';MART;True\n')
    elif MART_OK == 'False':
        #if count_data <= 7:
            #MESSAGE = MESSAGE + '\n------------------------------------------------\n' + DATA + ':\n--  FAILED   MART  -  backup is NOT succeeded!!!\n'
        if count_data == 1:
            f_MART.write('0' + '\n')
        MART_DBbckp_DICT[DATA] = ';MART;False'
        #f_SummLog.write(DATA + ';MART;False\n')


    if DWH_OK == 'True':
        if DWH_SHIFT >= TIME_SHIFT:
            if count_data == 1:
                f_DWH.write('1' + '\n')
            DWH_DBbckp_DICT[DATA] = ';DWH;True'
            #f_SummLog.write(DATA + ';DWH;True\n')
        else:
            if count_data == 1:
                f_DWH.write('1' + '\n')
            DWH_DBbckp_DICT[PreDay] = ';DWH;True'
    elif DWH_OK == 'False':
        #if count_data <= 7:
            #MESSAGE = MESSAGE + '--  FAILED   DWH   -  backup is NOT succeeded!!!\n'
        if count_data == 1:
            f_DWH.write('0' + '\n')
        DWH_DBbckp_DICT[DATA] = ';DWH;False'
        #f_SummLog.write(DATA + ';DWH;False\n')


    if DHUB_OK == 'True':
        if DATAHUB_SHIFT >= TIME_SHIFT:
            if count_data == 1:
                f_DHUB.write('1' + '\n')
            DHUB_DBbckp_DICT[DATA] = ';DHUB;True'
            #f_SummLog.write(DATA + ';DHUB;True\n')
        else:
            if count_data == 1:
                f_DHUB.write('1' + '\n')
            DHUB_DBbckp_DICT[PreDay] = ';DHUB;True'
    elif DHUB_OK == 'False':
        #if count_data <= 7:
            #MESSAGE = MESSAGE + '--  FAILED   DHUB  -  backup is NOT succeeded!!!'
        if count_data == 1:
            f_DHUB.write('0' + '\n')
        DHUB_DBbckp_DICT[DATA] = ';DHUB;False'
        #f_SummLog.write(DATA + ';DHUB;False\n')

for DATA in ALL_DATA:
    if  DATA in MART_DBbckp_DICT:
        f_SummLog.write(DATA + str(MART_DBbckp_DICT[DATA])+'\n')
    if  DATA in DWH_DBbckp_DICT:
        f_SummLog.write(DATA + str(DWH_DBbckp_DICT[DATA])+'\n')
    if  DATA in DHUB_DBbckp_DICT:
        f_SummLog.write(DATA + str(DHUB_DBbckp_DICT[DATA])+'\n')

f_MART.close()
f_DWH.close()
f_DHUB.close()



file.close()
f_SummLog.close()

#with open(CheckDBbckp_log, 'w') as f_ChDB:
#    inp = f_ChDB.writelines(MESSAGE)

if TEST_MODE:
    #print(MESSAGE)
    with open(SummaryLog_Name, 'r') as f_SummLog:
        for line in range(20):
            print(f_SummLog.readline())



##############################################################################################################################
###           CheckNetworker_v11.py - Paragraph <5> in the technical assignment                                            ###
##############################################################################################################################
if TEST_MODE:
    NSR_OUT_log = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\NSRINFO.txt'
    DWH_RUN_log = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\DWH_RUN_log.txt'
    MART_RUN_log = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\MART_RUN_log.txt'
    DATAHUB_RUN_log = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\DATAHUB_RUN_log.txt'
    DB_BAckupLog_Name = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\DB_BAckup.log'
    CheckNetworker_log = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\check_NETWORKER.txt'
else:
    NSR_OUT_log = '/var/logs/NSRINFO.txt'
    DWH_RUN_log = '/var/logs/DWH_RUN_log.txt'
    MART_RUN_log = '/var/logs/MART_RUN_log.txt'
    DATAHUB_RUN_log = '/var/logs/DATAHUB_RUN_log.txt'
    DB_BAckupLog_Name = '/var/logs/DB_BAckup.log'
    CheckNetworker_log = '/var/logs/check_NETWORKER.txt'

DataToday = str(datetime.date.today())
DataToday = DataToday.replace('-', '')
DataYesterday = str(datetime.date.today() - datetime.timedelta(days=1))
DataYesterday = DataYesterday.replace('-', '')

N = ['NODE0000', 'NODE0001', 'NODE0002', 'NODE0003', 'NODE0004', 'NODE0005', 'NODE0006', 'NODE0007', 'NODE0008',
     'NODE0009', 'NODE0010']
DWH_STR = []
MART_STR = []
DATAHUB_STR = []
MESSAGE_DWH = ''
MESSAGE_MART = ''
MESSAGE_DATAHUB = ''
ALL_DATA = set()
DWH_REPORT = [[], []]
DWH_REPORT_SHIFT = [[], []]
DWH_REPORT_DICT = {}
MART_REPORT = [[], []]
MART_REPORT_SHIFT = [[], []]
MART_REPORT_DICT = {}
DATAHUB_REPORT = [[], []]
DATAHUB_REPORT_SHIFT = [[], []]
DATAHUB_REPORT_DICT = {}
DWH_NODE = [False, False, False, False, False]
DWH_NODE_SHIFT = [False, False, False, False, False]
MART_NODE = [False, False, False, False, False, False, False, False, False, False, False]
MART_NODE_SHIFT = [False, False, False, False, False, False, False, False, False, False, False]
DATAHUB_NODE = [False, False, False, False, False, False, False, False, False, False, False]
DATAHUB_NODE_SHIFT = [False, False, False, False, False, False, False, False, False, False, False]
DWH_RUN_NOW = False
MART_RUN_NOW = False
DATAHUB_RUN_NOW = False
MESSAGE = '[]'
# with open(CheckNetworker_log, 'a') as log_file:
#    with redirect_stdout(log_file):
with redirect_stdout(sys.stderr):
    if not TEST_MODE:
        subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)
        strcmd = 'nsrinfo -v -s << ADDRES >> -n db2 -X all rsb-dbpdpfdr1  > ' + NSR_OUT_log + \
                 ';' \
                 '' \
                 '' \
                 ' >> ' + NSR_OUT_log + \
                 ';nsrinfo -v -s << ADDRES >> -n db2 -X all rsb-dbpdpfdr3 >> ' + NSR_OUT_log
        subprocess.call(strcmd, shell=True)

        strcmd = 'sudo -iu db2khdci /db2home/db2khdci/sqllib/adm/db2pd -util -dbp all  | grep bytes > ' + DWH_RUN_log + \
                 ';sudo -iu db2marti /db2home/db2khdci/sqllib/adm/db2pd -util -dbp all | grep bytes > ' + MART_RUN_log + \
                 ';sudo -iu db2dhpi  /db2home/db2khdci/sqllib/adm/db2pd -util -dbp all | grep bytes > ' + DATAHUB_RUN_log
        subprocess.call(strcmd, shell=True)


    f_DB_BAckupLog = open(DB_BAckupLog_Name, 'r')
    DB_BAckupLog_txt = f_DB_BAckupLog.readlines()

    ALL_DATA.add(DataToday)
    ALL_DATA.add(DataYesterday)

    with open(NSR_OUT_log) as f:
        inp = f.readlines()
    for i in iter(inp):
        pos = i.find('/DB_BACKUP.')
        if pos != -1:
            ALL_DATA.add(i[pos + 11:pos + 19])
    ALL_DATA = sorted(ALL_DATA, reverse=True)


    for ii in ALL_DATA:
        DWH_REPORT[0].append(ii)
        DWH_REPORT_SHIFT[0].append(ii)
        MART_REPORT[0].append(ii)
        MART_REPORT_SHIFT[0].append(ii)
        DATAHUB_REPORT[0].append(ii)
        DATAHUB_REPORT_SHIFT[0].append(ii)
    ####################################################################################
    with open(DWH_RUN_log) as f2:
        inp2 = f2.readlines()
    for i in iter(inp2):
        if 'bytes' in i:
            DWH_RUN_NOW = True

    with open(MART_RUN_log) as f3:
        inp3 = f3.readlines()
    for i in iter(inp3):
        if 'bytes' in i:
            MART_RUN_NOW = True
            # print('MART_RUN_NOW = True')

    with open(DATAHUB_RUN_log) as f4:
        inp4 = f4.readlines()
    for i in iter(inp4):
        if 'bytes' in i:
            DATAHUB_RUN_NOW = True
    ####################################################################################
    for nn in DWH_REPORT[0]:
        DWH_REPORT[1].append('Failed')
        DWH_REPORT_SHIFT[1].append('Failed')

    count = 0
    while count < len(ALL_DATA):  # len(DWH_REPORT[0]):
        for mm in range(len(DWH_NODE)):       DWH_NODE[mm] = False
        for mm in range(len(DWH_NODE_SHIFT)): DWH_NODE_SHIFT[mm] = False
        data_ind = ALL_DATA[count]
        for iii in iter(inp):
            if data_ind in iii:
                if 'DWH' in iii:
                    ##################################################################################################################
                    ##################################################################################################################
                    pos = iii.find('/DB_BACKUP.')
                    if pos != -1:
                        TimeBckp = iii[pos + 19:pos + 21]
                        if int(TimeBckp) < TIME_SHIFT:
                            if N[0] in iii:
                                DWH_NODE_SHIFT[0] = True
                            elif N[1] in iii:
                                DWH_NODE_SHIFT[1] = True
                            elif N[2] in iii:
                                DWH_NODE_SHIFT[2] = True
                            elif N[3] in iii:
                                DWH_NODE_SHIFT[3] = True
                            elif N[4] in iii:
                                DWH_NODE_SHIFT[4] = True
                        else:
                            if N[0] in iii:
                                DWH_NODE[0] = True
                            elif N[1] in iii:
                                DWH_NODE[1] = True
                            elif N[2] in iii:
                                DWH_NODE[2] = True
                            elif N[3] in iii:
                                DWH_NODE[3] = True
                            elif N[4] in iii:
                                DWH_NODE[4] = True

        if DWH_NODE[0] and DWH_NODE[1] and DWH_NODE[2] and DWH_NODE[3] and DWH_NODE[4]:
            DWH_REPORT[1][count] = 'Ok'

        NotStartedFlg = True
        for txt_ln in iter(DB_BAckupLog_txt):
            strdt1 = data_ind[0:4] + '-' + data_ind[4:6] + '-' + data_ind[6:8]
            if strdt1 in txt_ln:
                if 'DWH' in txt_ln:
                    NotStartedFlg = False
                    if 'False' in txt_ln:
                        DWH_REPORT[1][count] = 'Failed'
        if NotStartedFlg:
            DWH_REPORT[1][count] = 'Not Started'

        if DWH_NODE_SHIFT[0] and DWH_NODE_SHIFT[1] and DWH_NODE_SHIFT[2] and DWH_NODE_SHIFT[3] and DWH_NODE_SHIFT[4]:
            DWH_REPORT_SHIFT[1][count] = 'Ok'

        NotStartedFlg = True
        data_ind_mod = datetime.datetime(int(data_ind[:4]), int(data_ind[4:6]), int(data_ind[6:8]))
        PreDay = str((data_ind_mod - datetime.timedelta(days=1)).date())  # .replace('-', '')
        for txt_ln in iter(DB_BAckupLog_txt):
            if PreDay in txt_ln:
                if 'DWH' in txt_ln:
                    NotStartedFlg = False
                    if 'False' in txt_ln:
                        DWH_REPORT_SHIFT[1][count] = 'Failed'
        if NotStartedFlg:
            DWH_REPORT_SHIFT[1][count] = 'Not Started'

        count += 1

    ####################################################################################
    for nn in MART_REPORT[0]:
        MART_REPORT[1].append('Failed')  # Not Running')
        MART_REPORT_SHIFT[1].append('Failed')

    count = 0
    while count < len(ALL_DATA):  # len(MART_REPORT[0]):
        for mm in range(len(MART_NODE)):       MART_NODE[mm] = False
        for mm in range(len(MART_NODE_SHIFT)): MART_NODE_SHIFT[mm] = False
        data_ind = ALL_DATA[count]
        for iii in iter(inp):
            if data_ind in iii:
                if 'MART' in iii:
                    ##################################################################################################################
                    ##################################################################################################################
                    pos = iii.find('/DB_BACKUP.')
                    if pos != -1:
                        TimeBckp = iii[pos + 19:pos + 21]
                        if int(TimeBckp) < TIME_SHIFT:
                            if N[0] in iii:
                                MART_NODE_SHIFT[0] = True
                            elif N[1] in iii:
                                MART_NODE_SHIFT[1] = True
                            elif N[2] in iii:
                                MART_NODE_SHIFT[2] = True
                            elif N[3] in iii:
                                MART_NODE_SHIFT[3] = True
                            elif N[4] in iii:
                                MART_NODE_SHIFT[4] = True
                            elif N[5] in iii:
                                MART_NODE_SHIFT[5] = True
                            elif N[6] in iii:
                                MART_NODE_SHIFT[6] = True
                            elif N[7] in iii:
                                MART_NODE_SHIFT[7] = True
                            elif N[8] in iii:
                                MART_NODE_SHIFT[8] = True
                            elif N[9] in iii:
                                MART_NODE_SHIFT[9] = True
                            elif N[10] in iii:
                                MART_NODE_SHIFT[10] = True
                        else:
                            if N[0] in iii:
                                MART_NODE[0] = True
                            elif N[1] in iii:
                                MART_NODE[1] = True
                            elif N[2] in iii:
                                MART_NODE[2] = True
                            elif N[3] in iii:
                                MART_NODE[3] = True
                            elif N[4] in iii:
                                MART_NODE[4] = True
                            elif N[5] in iii:
                                MART_NODE[5] = True
                            elif N[6] in iii:
                                MART_NODE[6] = True
                            elif N[7] in iii:
                                MART_NODE[7] = True
                            elif N[8] in iii:
                                MART_NODE[8] = True
                            elif N[9] in iii:
                                MART_NODE[9] = True
                            elif N[10] in iii:
                                MART_NODE[10] = True

        # MART_REPORT[1][count] = 'Not Started'
        if MART_NODE[0] and MART_NODE[1] and MART_NODE[2] and MART_NODE[3] \
                and MART_NODE[4] and MART_NODE[5] and MART_NODE[6] and MART_NODE[7] \
                and MART_NODE[8] and MART_NODE[9] and MART_NODE[10]:
            MART_REPORT[1][count] = 'Ok'

        NotStartedFlg = True
        strdt1 = data_ind[0:4] + '-' + data_ind[4:6] + '-' + data_ind[6:8]
        for txt_ln in iter(DB_BAckupLog_txt):
            if strdt1 in txt_ln:
                if 'MART' in txt_ln:
                    NotStartedFlg = False
                    if 'False' in txt_ln:
                        MART_REPORT[1][count] = 'Failed'

        if NotStartedFlg:
            MART_REPORT[1][count] = 'Not Started'

        if MART_NODE_SHIFT[0] and MART_NODE_SHIFT[1] and MART_NODE_SHIFT[2] and MART_NODE_SHIFT[3] \
                and MART_NODE_SHIFT[4] and MART_NODE_SHIFT[5] and MART_NODE_SHIFT[6] and MART_NODE_SHIFT[7] \
                and MART_NODE_SHIFT[8] and MART_NODE_SHIFT[9] and MART_NODE_SHIFT[10]:
            MART_REPORT_SHIFT[1][count] = 'Ok'

        NotStartedFlg = True
        data_ind_mod = datetime.datetime(int(data_ind[:4]), int(data_ind[4:6]), int(data_ind[6:8]))
        PreDay = str((data_ind_mod - datetime.timedelta(days=1)).date())  # .replace('-', '')
        for txt_ln in iter(DB_BAckupLog_txt):
            if PreDay in txt_ln:
                if 'MART' in txt_ln:
                    NotStartedFlg = False
                    if 'False' in txt_ln:
                        MART_REPORT_SHIFT[1][count] = 'Failed'

        if NotStartedFlg:
            MART_REPORT_SHIFT[1][count] = 'Not Started'

        count += 1

    ####################################################################################
    for nn in DATAHUB_REPORT[0]:
        DATAHUB_REPORT[1].append('Failed')  # Not Running')       #('FAILED')
        DATAHUB_REPORT_SHIFT[1].append('Failed')

    count = 0
    while count < len(ALL_DATA):  # len(DATAHUB_REPORT[0]):
        for mm in range(len(DATAHUB_NODE)):       DATAHUB_NODE[mm] = False
        for mm in range(len(DATAHUB_NODE_SHIFT)): DATAHUB_NODE_SHIFT[mm] = False
        data_ind = ALL_DATA[count]
        for iii in iter(inp):
            if data_ind in iii:
                if 'DATAHUB' in iii:
                    ##################################################################################################################
                    ##################################################################################################################
                    pos = iii.find('/DB_BACKUP.')
                    if pos != -1:
                        TimeBckp = iii[pos + 19:pos + 21]
                        if int(TimeBckp) < TIME_SHIFT:
                            if N[0] in iii:
                                DATAHUB_NODE_SHIFT[0] = True
                            elif N[1] in iii:
                                DATAHUB_NODE_SHIFT[1] = True
                            elif N[2] in iii:
                                DATAHUB_NODE_SHIFT[2] = True
                            elif N[3] in iii:
                                DATAHUB_NODE_SHIFT[3] = True
                            elif N[4] in iii:
                                DATAHUB_NODE_SHIFT[4] = True
                            elif N[5] in iii:
                                DATAHUB_NODE_SHIFT[5] = True
                            elif N[6] in iii:
                                DATAHUB_NODE_SHIFT[6] = True
                            elif N[7] in iii:
                                DATAHUB_NODE_SHIFT[7] = True
                            elif N[8] in iii:
                                DATAHUB_NODE_SHIFT[8] = True
                            elif N[9] in iii:
                                DATAHUB_NODE_SHIFT[9] = True
                        else:
                            if N[0] in iii:
                                DATAHUB_NODE[0] = True
                            elif N[1] in iii:
                                DATAHUB_NODE[1] = True
                            elif N[2] in iii:
                                DATAHUB_NODE[2] = True
                            elif N[3] in iii:
                                DATAHUB_NODE[3] = True
                            elif N[4] in iii:
                                DATAHUB_NODE[4] = True
                            elif N[5] in iii:
                                DATAHUB_NODE[5] = True
                            elif N[6] in iii:
                                DATAHUB_NODE[6] = True
                            elif N[7] in iii:
                                DATAHUB_NODE[7] = True
                            elif N[8] in iii:
                                DATAHUB_NODE[8] = True
                            elif N[9] in iii:
                                DATAHUB_NODE[9] = True

        # DATAHUB_REPORT[1][count] = 'Not Started'
        if DATAHUB_NODE[0] and DATAHUB_NODE[1] and DATAHUB_NODE[2] and DATAHUB_NODE[3] \
                and DATAHUB_NODE[4] and DATAHUB_NODE[5] and DATAHUB_NODE[6] and DATAHUB_NODE[7] \
                and DATAHUB_NODE[8] and DATAHUB_NODE[9]:
            DATAHUB_REPORT[1][count] = 'Ok'

        NotStartedFlg = True
        strdt1 = data_ind[0:4] + '-' + data_ind[4:6] + '-' + data_ind[6:8]
        for txt_ln in iter(DB_BAckupLog_txt):
            if strdt1 in txt_ln:
                if 'DHUB' in txt_ln:
                    NotStartedFlg = False
                    if 'False' in txt_ln:
                        DATAHUB_REPORT[1][count] = 'Failed'

        if NotStartedFlg:
            DATAHUB_REPORT[1][count] = 'Not Started'

        if DATAHUB_NODE_SHIFT[0] and DATAHUB_NODE_SHIFT[1] and DATAHUB_NODE_SHIFT[2] and DATAHUB_NODE_SHIFT[3] \
                and DATAHUB_NODE_SHIFT[4] and DATAHUB_NODE_SHIFT[5] and DATAHUB_NODE_SHIFT[6] and DATAHUB_NODE_SHIFT[7] \
                and DATAHUB_NODE_SHIFT[8] and DATAHUB_NODE_SHIFT[9]:
            DATAHUB_REPORT_SHIFT[1][count] = 'Ok'

        NotStartedFlg = True
        data_ind_mod = datetime.datetime(int(data_ind[:4]), int(data_ind[4:6]), int(data_ind[6:8]))
        PreDay = str((data_ind_mod - datetime.timedelta(days=1)).date())  # .replace('-', '')
        for txt_ln in iter(DB_BAckupLog_txt):
            if PreDay in txt_ln:
                if 'DHUB' in txt_ln:
                    NotStartedFlg = False
                    if 'False' in txt_ln:
                        DATAHUB_REPORT_SHIFT[1][count] = 'Failed'

        if NotStartedFlg:
            DATAHUB_REPORT_SHIFT[1][count] = 'Not Started'

        count += 1

    ####################################################################################


    PreDay = ''
    count2 = 0
    while count2 < len(DWH_REPORT[0]):
        if PreDay != DWH_REPORT[0][count2]:
            DWH_REPORT_DICT[DWH_REPORT[0][count2]] = DWH_REPORT[1][count2]
        if DWH_REPORT_SHIFT[1][count2] == 'Ok' or DWH_REPORT_SHIFT[1][count2] == 'Not Started':
            data_ind_mod = datetime.datetime(int(DWH_REPORT_SHIFT[0][count2][:4]),
                                             int(DWH_REPORT_SHIFT[0][count2][4:6]),
                                             int(DWH_REPORT_SHIFT[0][count2][6:8]))
            PreDay = str((data_ind_mod - datetime.timedelta(days=1)).date()).replace('-', '')
            DWH_REPORT_DICT[PreDay] = DWH_REPORT_SHIFT[1][count2]
            ALL_DATA = set(ALL_DATA)
            ALL_DATA.add(PreDay)
            ALL_DATA = sorted(ALL_DATA, reverse=True)
        count2 += 1

    PreDay = ''
    count2 = 0
    while count2 < len(MART_REPORT[0]):
        if PreDay != MART_REPORT[0][count2]:
            MART_REPORT_DICT[MART_REPORT[0][count2]] = MART_REPORT[1][count2]
        if MART_REPORT_SHIFT[1][count2] == 'Ok' or MART_REPORT_SHIFT[1][count2] == 'Not Started':
            data_ind_mod = datetime.datetime(int(MART_REPORT_SHIFT[0][count2][:4]),
                                             int(MART_REPORT_SHIFT[0][count2][4:6]),
                                             int(MART_REPORT_SHIFT[0][count2][6:8]))
            PreDay = str((data_ind_mod - datetime.timedelta(days=1)).date()).replace('-', '')
            MART_REPORT_DICT[PreDay] = MART_REPORT_SHIFT[1][count2]
            ALL_DATA = set(ALL_DATA)
            ALL_DATA.add(PreDay)
            ALL_DATA = sorted(ALL_DATA, reverse=True)
        count2 += 1

    PreDay = ''
    count2 = 0
    while count2 < len(DATAHUB_REPORT[0]):
        if PreDay != DATAHUB_REPORT[0][count2]:
            DATAHUB_REPORT_DICT[DATAHUB_REPORT[0][count2]] = DATAHUB_REPORT[1][count2]
        if DATAHUB_REPORT_SHIFT[1][count2] == 'Ok' or DATAHUB_REPORT_SHIFT[1][count2] == 'Not Started':
            data_ind_mod = datetime.datetime(int(DATAHUB_REPORT_SHIFT[0][count2][:4]),
                                             int(DATAHUB_REPORT_SHIFT[0][count2][4:6]),
                                             int(DATAHUB_REPORT_SHIFT[0][count2][6:8]))
            PreDay = str((data_ind_mod - datetime.timedelta(days=1)).date()).replace('-', '')
            DATAHUB_REPORT_DICT[PreDay] = DATAHUB_REPORT_SHIFT[1][count2]
            ALL_DATA = set(ALL_DATA)
            ALL_DATA.add(PreDay)
            ALL_DATA = sorted(ALL_DATA, reverse=True)
        count2 += 1

    for cntdata in ALL_DATA:
        if not (cntdata in DWH_REPORT_DICT): DWH_REPORT_DICT[cntdata] = 'Failed'
        if not (cntdata in MART_REPORT_DICT): MART_REPORT_DICT[cntdata] = 'Failed'
        if not (cntdata in DATAHUB_REPORT_DICT): DATAHUB_REPORT_DICT[cntdata] = 'Failed'

    ####################################################################################
    if CurHour < TIME_SHIFT:
        if ALL_DATA[0] == DataToday:
            ALL_DATA = ALL_DATA[1:]


    ####################################################################################
    if DWH_RUN_NOW and  int(CurHour) < TIME_SHIFT: #DWH_REPORT[0][0] == DataYesterday:
        DWH_REPORT_DICT[DataYesterday] = 'InProgress'
    elif DWH_RUN_NOW and  int(CurHour) >= TIME_SHIFT:
        DWH_REPORT_DICT[DataToday] = 'InProgress'

    #if int(CurHour) < TIME_SHIFT:

    if MART_RUN_NOW and int(CurHour) < TIME_SHIFT: #MART_REPORT[0][0] == DataYesterday:
        MART_REPORT_DICT[DataYesterday] = 'InProgress'
    elif MART_RUN_NOW and int(CurHour) >= TIME_SHIFT:
        MART_REPORT_DICT[DataToday] = 'InProgress'

    if DATAHUB_RUN_NOW and int(CurHour) < TIME_SHIFT: #DATAHUB_REPORT[0][0] == DataYesterday:
        DATAHUB_REPORT_DICT[DataYesterday] = 'InProgress'
    elif DATAHUB_RUN_NOW and int(CurHour) >= TIME_SHIFT:
        DATAHUB_REPORT_DICT[DataToday] = 'InProgress'
    ####################################################################################



    MESSAGE = '['

    for count in range(len(ALL_DATA)):  # Проходим в цикле по всем датам и формируем отчет.
        strdt = ALL_DATA[
            count]  # DWH_REPORT[0][count],MART_REPORT[0][count],DATAHUB_REPORT[0][count],ALL_DATA[count] - это все текущая в цикле дата
        strdt = strdt[6:8] + '.' + strdt[4:6] + '.' + strdt[0:4]
        MESSAGE = MESSAGE + '\n {\n  "DATE": "' + strdt + '",'
        MESSAGE = MESSAGE + '\n  "DWH": "' + str(DWH_REPORT_DICT[ALL_DATA[count]]) + '",'
        # strdt = MART_REPORT[0][count]
        # strdt = strdt[0:4] + '-' + strdt[4:6] + '-' + strdt[6:8]
        MESSAGE = MESSAGE + '\n  "MART": "' + str(MART_REPORT_DICT[ALL_DATA[count]]) + '",'
        # strdt = DATAHUB_REPORT[0][count]
        # strdt = strdt[0:4] + '-' + strdt[4:6] + '-' + strdt[6:8]
        MESSAGE = MESSAGE + '\n  "DATAHUB": "' + str(DATAHUB_REPORT_DICT[ALL_DATA[count]]) + '"\n },'
    MESSAGE = MESSAGE[:-1]
    MESSAGE = MESSAGE + '\n]'

    # print( ' ---------  IN LOG ----------- =>>>'+MESSAGE) # Test in consol

    # MESSAGE = "Paragraph < 5 > in the technical assignment :\n  BACKUPS ARE FOUND STORED IN NETWORKER : \n---------------------------\n" + MESSAGE
    # log_file.writelines(MESSAGE)
    # print('test ERROR OUT')

    f.close()
    f_DB_BAckupLog.close()

print(MESSAGE)  # , file=stdout) #  in stdout

########################################################################################################################################################################


