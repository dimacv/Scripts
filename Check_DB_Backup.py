#!/usr/bin/python3
### Check_DB_Backup_v2.py - Paragraph 3 in the technical assignment  ###
import os
import datetime
import subprocess



MAIL = '<< MAIL >>'

DEPH1= 12
subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)
DataToday = str(datetime.date.today())
ThisYear = DataToday[:4]
DataYesterday = str(datetime.date.today() - datetime.timedelta(days=1) )
DIR_init_db_log = '/db2home/scripts/init_db/log/'
FLAG_DWH = '/tmp/flag_DWH_backup.flg'
FLAG_MART = '/tmp/flag_MART_backup.flg'
FLAG_DHUB = '/tmp/flag_DATAHUB_backup.flg'
SummaryLog_Name = '/var/log/DB_BAckup.log'
CheckDBbckp_log = '/var/logs/check_DB_backup.txt'
DWH_OK=False
DHUB_OK=False
MART_OK=False
MART_D_OK=False
DWH_D_OK=False
DHUB_D_OK=False
FILE_MART_LOG = ''
FILE_DWH_LOG = ''
FILE_DATAHUB_LOG = ''
MESSAGE = ''
STR1=[]
ALL_DATA = set()

def enumeratefiles(path):
    file_collection = []
    for dirpath, dirnames, filenames in os.walk(path):
        for file in filenames:
            file_collection.append(file)
    return file_collection

FILE_LOGS_NAME = enumeratefiles(DIR_init_db_log)
for i in FILE_LOGS_NAME:
    if ThisYear in i[:4]:
        ALL_DATA.add(i[:10])
ALL_DATA = sorted(ALL_DATA,reverse=True)
#if ALL_DATA[0] ==  DataToday :
#    ALL_DATA = ALL_DATA[1:]
f_SummLog = open(SummaryLog_Name, 'w')
f_SummLog.close()

count_data = 0
for DATA in ALL_DATA:
    count_data = count_data + 1
    DWH_OK = False
    DHUB_OK = False
    MART_OK = False
    MART_D_OK = False
    DWH_D_OK = False
    DHUB_D_OK = False

    for i in FILE_LOGS_NAME:
        if DATA in i :
            if 'MART' in i:
                FILE_MART_LOG = i
            elif 'DWH' in i:
                FILE_DWH_LOG = i
            elif  'DATAHUB' in i:
                FILE_DATAHUB_LOG = i


    if FILE_MART_LOG == '':
        print('FAILED !!!.  MART - backup is NOT succeeded (not found the log of the corresponding backup) !!!')
    else:
        file = open(DIR_init_db_log + FILE_MART_LOG)
        inp = file.readlines()
        for ii in iter(inp):
            STR1.append(ii)
            if '+ exit 0' in ii :# and 'NMDA backup was successful.' in STR1[-9]:
                for iii in range(DEPH1):
                    if 'NMDA backup was successful.' in STR1[-iii]:
                        MART_OK = True


    if FILE_DWH_LOG == '':
        print('FAILED !!!.  DWH - backup is NOT succeeded (not found the log of the corresponding backup) !!!')
    else:
        file = open(DIR_init_db_log + FILE_DWH_LOG)
        inp = file.readlines()
        for ii in iter(inp):
            STR1.append(ii)
            if '+ exit 0' in ii :# and 'NMDA backup was successful.' in STR1[-9]:
                for iii in range(DEPH1):
                    if 'NMDA backup was successful.' in STR1[-iii]:
                        DWH_OK = True



    if FILE_DATAHUB_LOG =='':
        print('FAILED !!!.  DATAHUB - backup is NOT succeeded (not found the log of the corresponding backup) !!!')
    else:
        #print ('DATAHUB = ' + FILE_DATAHUB_LOG)
        file = open(DIR_init_db_log + FILE_DATAHUB_LOG)
        inp = file.readlines()
        for ii in iter(inp):
            STR1.append(ii)
            if '+ exit 0' in ii :
                for iii in range(DEPH1):
                    if 'NMDA backup was successful.' in STR1[-iii]:
                        DHUB_OK = True

    f_MART= open(FLAG_MART, 'w')
    f_DWH = open(FLAG_DWH, 'w')
    f_DHUB = open(FLAG_DHUB, 'w')
    f_SummLog = open(SummaryLog_Name, 'a')

    if MART_OK:
        if count_data <=7:
            MESSAGE = MESSAGE+'\n------------------------------------------------\n'+DATA+':\n+      OK        MART  -  backup succeeded!\n'
        if count_data == 1:
            f_MART.write('1' + '\n')
        f_SummLog.write( DATA + ';MART;True\n')
    else:
        if count_data <= 7:
             MESSAGE = MESSAGE + '\n------------------------------------------------\n'+DATA + ':\n--  FAILED   MART  -  backup is NOT succeeded!!!\n'
        if count_data == 1:
             f_MART.write('0' + '\n')
        f_SummLog.write(DATA + ';MART;False\n')

    if DWH_OK:
        if count_data <= 7:
            MESSAGE = MESSAGE + '+      OK        DWH   -  backup succeeded!\n'
        if count_data == 1:
            f_DWH.write('1' + '\n')
        f_SummLog.write(DATA + ';DWH;True\n')
    else:
        if count_data <= 7:
            MESSAGE = MESSAGE + '--  FAILED   DWH   -  backup is NOT succeeded!!!\n'
        if count_data == 1:
            f_DWH.write('0' + '\n')
        f_SummLog.write(DATA + ';DWH;False\n')

    if DHUB_OK:
        if count_data <= 7:
            MESSAGE = MESSAGE + '+      OK        DHUB  -  backup succeeded!'
        if count_data == 1:
            f_DHUB.write('1' + '\n')
        f_SummLog.write(DATA + ';DHUB;True\n')
    else:
        if count_data <= 7:
            MESSAGE = MESSAGE + '--  FAILED   DHUB  -  backup is NOT succeeded!!!'
        if count_data == 1:
            f_DHUB.write('0' + '\n')
        f_SummLog.write(DATA + ';DHUB;False\n')

    f_MART.close()
    f_DWH.close()
    f_DHUB.close()

#print(MESSAGE)
cmd='echo "Paragraph < 3 > in the technical assignment : ' + MESSAGE + '" | mail -s ">>>--- Paragraph < 3 > in the technical assignment - MART,DWH,DATAHUB   DB BACKUP ---<<<" ' + MAIL
p=subprocess.call(cmd, shell=True)
file.close()
f_SummLog.close()

with open(CheckDBbckp_log,'w') as f_ChDB:
    inp = f_ChDB.writelines(MESSAGE)

###############################################################
'''
+   OK       MART  -  backup succeeded!
--  FAILED   MART  -  backup is NOT succeeded!!!
------------------------------------------------
+   OK       DWH   -  backup succeeded!
--  FAILED   DWH   -  backup is NOT succeeded!!!
+   OK       DHUB  -  backup succeeded!
--  FAILED   DHUB  -  backup is NOT succeeded!!!



'''
