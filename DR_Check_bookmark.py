#!/usr/bin/python3
### DR_Check_bookmark.py

import os
import subprocess

MAIL = '<< MAIL >>'

str1 = []
DHUB_OK = False
DWH_OK = False
MART_OK = False
FLAG_DWH = '/tmp/flag_DR_DWH_bookmark.flg'
FLAG_MART = '/tmp/flag_DR_MART_bookmark.flg'
FLAG_DHUB = '/tmp/flag_DR_DATAHUB_bookmark.flg'
FILE_LOG = '/tmp/DR_CHK_BKMRK.txt'
Checkbookmark_log = '/var/logs/check_bookmark_DR.txt'

subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)
subprocess.call('/db2home/scripts/init_db/check_snapshot_full_output.sh > ' + FILE_LOG + ' 2>/dev/null', shell=True)

file = open(FILE_LOG, 'r')
inp = file.readlines()

for i in iter(inp):
    str1.append(i)
    if 'Storage access: LOGGED ACCESS' in i and 'DHUB_CG' in str1[-11]:
        DHUB_OK = True

    if 'Storage access: LOGGED ACCESS' in i and 'DWH_CG' in str1[-11]:
        DWH_OK = True

    if 'Storage access: LOGGED ACCESS' in i and 'MART_CG' in str1[-11]:
        MART_OK = True


f_MART= open(FLAG_MART, 'w')
f_DWH = open(FLAG_DWH, 'w')
f_DHUB = open(FLAG_DHUB, 'w')

if DHUB_OK:
    MESSAGE = ' OK. DHUB_CG - DR bookmark todays succeeded!\n'
    f_DHUB.write('1' + '\n')
else:
    MESSAGE = 'FAILED !!!.  DHUB_CG  - DR bookmark todays is NOT complete!!!\n'
    f_DHUB.write('0' + '\n')

if DWH_OK:
    MESSAGE = MESSAGE + ' OK. DWH_CG  - DR bookmark todays succeeded!\n'
    f_DWH.write('1' + '\n')
else:
    MESSAGE = MESSAGE + 'FAILED !!!.  DWH_CG  - DR bookmark todays is NOT complete!!!\n'
    f_DWH.write('0' + '\n')

if MART_OK:
    MESSAGE = MESSAGE + ' OK. MART_CG - DR bookmark todays succeeded!'
    f_MART.write('1' + '\n')
else:
    MESSAGE = MESSAGE + 'FAILED !!!.  MART_CG  - DR bookmark todays is NOT complete!!!'
    f_MART.write('0' + '\n')

f_MART.close()
f_DWH.close()
f_DHUB.close()


print( MESSAGE) # Test in consol
cmd = 'echo "' + MESSAGE + '" | mail -s ">>>--- DR BOOKMARK TODAYS ---<<<" ' + MAIL
p = subprocess.call(cmd, shell=True)


file.close()

with open(Checkbookmark_log,'w') as f_Chbkm:
    inp = f_Chbkm.writelines(MESSAGE)



