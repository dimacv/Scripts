#!/usr/bin/python3
### CheckNetworker_v5.py - Paragraph < 5 > in the technical assignment   ###

import os
import subprocess
import datetime

MAIL = '<< EMAIL >>'

DataToday = str(datetime.date.today())
DataToday = DataToday.replace('-', '')
DataYesterday = str(datetime.date.today() - datetime.timedelta(days=1))
DataYesterday = DataYesterday.replace('-', '')
NSR_OUT_log = '/tmp/NSRINFO.txt'
DWH_RUN_log = '/tmp/DWH_RUN_log.txt'
MART_RUN_log = '/tmp/MART_RUN_log.txt'
DATAHUB_RUN_log = '/tmp/DATAHUB_RUN_log.txt'
DB_BAckupLog_Name = '/var/log/DB_BAckup.log'
CheckNetworker_log = '/var/logs/check_NETWORKER.txt'

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
MART_REPORT = [[], []]
DATAHUB_REPORT = [[], []]
DWH_NODE = [False, False, False, False, False]
MART_NODE = [False, False, False, False, False, False, False, False, False, False, False]
DATAHUB_NODE = [False, False, False, False, False, False, False, False, False, False, False]
DWH_RUN_NOW = False
MART_RUN_NOW = False
DATAHUB_RUN_NOW = False

subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)
strcmd = 'nsrinfo -v -s << ADDRES >> -n db2 -X all rsb-dbpdpfdr1  > ' + NSR_OUT_log + \
         ';nsrinfo -v -s << ADDRES >> -n db2 -X all rsb-dbpdpfdr2 >> ' + NSR_OUT_log + \
         ';nsrinfo -v -s << ADDRES >> -n db2 -X all rsb-dbpdpfdr3 >> ' + NSR_OUT_log
subprocess.call(strcmd, shell=True)

strcmd = 'sudo -iu db2khdci /db2home/db2khdci/sqllib/adm/db2pd -util -dbp all  | grep bytes > ' + DWH_RUN_log + \
         ';sudo -iu db2marti /db2home/db2khdci/sqllib/adm/db2pd -util -dbp all | grep bytes > ' + MART_RUN_log + \
         ';sudo -iu db2dhpi  /db2home/db2khdci/sqllib/adm/db2pd -util -dbp all | grep bytes > ' + DATAHUB_RUN_log
subprocess.call(strcmd, shell=True)


f_DB_BAckupLog = open(DB_BAckupLog_Name,'r')
DB_BAckupLog_txt = f_DB_BAckupLog.readlines()

with open(NSR_OUT_log) as f:
    inp = f.readlines()
for i in iter(inp):
    pos = i.find('/DB_BACKUP.')
    if pos != -1:
        ALL_DATA.add(i[pos + 11:pos + 19])
ALL_DATA = sorted(ALL_DATA, reverse=True)

if ALL_DATA[0] == DataToday:     ALL_DATA = ALL_DATA[1:]
#if ALL_DATA[0] == DataYesterday: ALL_DATA = ALL_DATA[1:]

for ii in ALL_DATA:
    DWH_REPORT[0].append(ii)
    MART_REPORT[0].append(ii)
    DATAHUB_REPORT[0].append(ii)
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
    DWH_REPORT[1].append('FAILED')# ('Not Running')       #'FAILED')

count = 0
while count < len(DWH_REPORT[0]):
    for mm in DWH_NODE: DWH_NODE[mm] = False
    data_ind = DWH_REPORT[0][count]
    for iii in iter(inp):
        if data_ind in iii:
            if 'DWH' in iii:
                DWH_REPORT[1][count] = 'FAILED'
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
        for txt_ln in iter(DB_BAckupLog_txt):
            strdt1 = data_ind[0:4] + '-' + data_ind[4:6] + '-' + data_ind[6:8]
            if strdt1 in txt_ln:
                  if 'DWH' in txt_ln:
                      if 'False' in txt_ln:
                          DWH_REPORT[1][count] = 'FAILED'


    count += 1

####################################################################################
for nn in MART_REPORT[0]:
    MART_REPORT[1].append('FAILED')# ('Not Running')       #('FAILED')

count = 0
while count < len(MART_REPORT[0]):
    for mm in MART_NODE: MART_NODE[mm] = False
    data_ind = MART_REPORT[0][count]
    for iii in iter(inp):
        if data_ind in iii:
            if 'MART' in iii:
                MART_REPORT[1][count] = 'FAILED'
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

    if MART_NODE[0] and MART_NODE[1] and MART_NODE[2] and MART_NODE[3] \
            and MART_NODE[4] and MART_NODE[5] and MART_NODE[6] and MART_NODE[7] \
            and MART_NODE[8] and MART_NODE[9] and MART_NODE[10]:
        MART_REPORT[1][count] = 'Ok'
        for txt_ln in iter(DB_BAckupLog_txt):
            strdt1 = data_ind[0:4] + '-' + data_ind[4:6] + '-' + data_ind[6:8]
            if strdt1 in txt_ln:
                  if 'MART' in txt_ln:
                      if 'False' in txt_ln:
                          MART_REPORT[1][count] = 'FAILED'
    count += 1

####################################################################################
for nn in DATAHUB_REPORT[0]:
    DATAHUB_REPORT[1].append('FAILED')# 'Not Running')       #('FAILED')

count = 0
while count < len(DATAHUB_REPORT[0]):
    for mm in DATAHUB_NODE: DATAHUB_NODE[mm] = False
    data_ind = DATAHUB_REPORT[0][count]
    for iii in iter(inp):
        if data_ind in iii:
            if 'DATAHUB' in iii:
                DATAHUB_REPORT[1][count] = 'FAILED'
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

    if DATAHUB_NODE[0] and DATAHUB_NODE[1] and DATAHUB_NODE[2] and DATAHUB_NODE[3] \
            and DATAHUB_NODE[4] and DATAHUB_NODE[5] and DATAHUB_NODE[6] and DATAHUB_NODE[7] \
            and DATAHUB_NODE[8] and DATAHUB_NODE[9]:
        DATAHUB_REPORT[1][count] = 'Ok'
        for txt_ln in iter(DB_BAckupLog_txt):
            strdt1 = data_ind[0:4] + '-' + data_ind[4:6] + '-' + data_ind[6:8]
            if strdt1 in txt_ln:
                  if 'DHUB' in txt_ln:
                      if 'False' in txt_ln:
                          DATAHUB_REPORT[1][count] = 'FAILED'

    count += 1

# if DWH_REPORT[1][0] == 'FAILED' and
if DWH_RUN_NOW and DWH_REPORT[0][0] == DataYesterday:
    DWH_REPORT[1][0] = 'PROGRESS'

# if  MART_REPORT[1][0] == 'FAILED' and
if MART_RUN_NOW and MART_REPORT[0][0] == DataYesterday:
    MART_REPORT[1][0] = 'PROGRESS'
    #print('MART_REPORT[1][0] = PROGRESS')

# if  DATAHUB_REPORT[1][0] == 'FAILED' and
if DATAHUB_RUN_NOW and DATAHUB_REPORT[0][0] == DataYesterday:
    DATAHUB_REPORT[1][0] = 'PROGRESS'

####################################################################################




####################################################################################
MESSAGE = ''
for count in range(len(ALL_DATA)):
    strdt = DWH_REPORT[0][count]
    strdt = strdt[0:4] + '-' + strdt[4:6] + '-' + strdt[6:8]
    MESSAGE = MESSAGE + str(strdt + " : \nDWH  -   " + str(DWH_REPORT[1][count]))
    strdt = MART_REPORT[0][count]
    strdt = strdt[0:4] + '-' + strdt[4:6] + '-' + strdt[6:8]
    MESSAGE = MESSAGE + str("\nMART  -   " + str(MART_REPORT[1][count]))
    strdt = DATAHUB_REPORT[0][count]
    strdt = strdt[0:4] + '-' + strdt[4:6] + '-' + strdt[6:8]
    MESSAGE = MESSAGE + str("\nDATAHUB  -   " + str(DATAHUB_REPORT[1][count])) + '\n---------------------------\n'

print( MESSAGE) # Test in consol
cmd = 'echo "Paragraph < 5 > in the technical assignment :\n  BACKUPS ARE FOUND STORED IN NETWORKER : \n---------------------------\n' + MESSAGE + '" | mail -s ">>>---  BACKUPS ARE FOUND STORED IN NETWORKER: ---<<<" ' + MAIL
p = subprocess.call(cmd, shell=True)

MESSAGE = "Paragraph < 5 > in the technical assignment :\n  BACKUPS ARE FOUND STORED IN NETWORKER : \n---------------------------\n" + MESSAGE
with open(CheckNetworker_log,'w') as f_ChNet:
    inp = f_ChNet.writelines(MESSAGE)

f.close()
f_DB_BAckupLog.close()

