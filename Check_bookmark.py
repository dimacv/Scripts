#!/usr/bin/python3
#########  Check_bookmark.py  ##########
import os
import datetime
import subprocess

MAIL = 'Dmitriy.Tsvirkun@rosbank.ru, Ilya.Mazurin@rosbank.ru, Konstantin.Shchepetev@rosbank.ru'
#MAIL = 'Dmitriy.Tsvirkun@rosbank.ru'
CheckBookmark_log = '/var/logs/check_bookmark-DPF1.txt'
DataToday = str(datetime.date.today())
STR = ['','','']
BCKP_BOOKM=False
subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)

inp = open('/db2home/scripts/DB2_RP.sh.log', 'r',encoding='cp1251').readlines()
f = open('/tmp/flag_bookmark.flg','w')

for i in iter(inp):
    STR[2]=STR[1]
    STR[1] = STR[0]
    STR[0] = i
    if DataToday  in i and 'Request for bookmark registered successfully.' in STR[2]:
        BCKP_BOOKM=True



if BCKP_BOOKM:
    MESSAGE = 'OK. Todays - ' +  DataToday + ' - bookmark backup succeeded!'
    cmd='echo "OK. Todays - ' +  DataToday + ' - bookmark backup succeeded!" | mail -s "OK. Todays -' +  DataToday + ' - bookmark backup succeeded!" ' + MAIL
    p=subprocess.call(cmd, shell=True)
    f.write('1' + '\n')
else:
    MESSAGE = 'FAILED !!!.  Todays - ' +  DataToday + ' -  bookmark is NOT complete!!! '
    cmd='echo "FAILED !!!.  Todays - ' +  DataToday + ' -  bookmark is NOT complete!!! " | mail -s "FAILED !!!. Todays ' +  DataToday + ' - bookmark is NOT complete!!!" '  + MAIL
    p=subprocess.call(cmd, shell=True)
    f.write('0' + '\n')

f.close()
with open(CheckBookmark_log,'w') as f_ChNet:
    inp = f_ChNet.writelines(MESSAGE)

subprocess.call('scp /var/logs/check_bookmark-DPF1.txt rsb-dbpdpfdr1:/var/logs/check_bookmark-DPF1.txt', shell=True)

