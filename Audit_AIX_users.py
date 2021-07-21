#!/usr/bin/python3
###  Audit_AIX_users.py   ###

from __future__ import print_function
import os
import platform
import sys
import subprocess
import datetime
from contextlib import redirect_stdout
from datetime import datetime
import re


outsep = " ; "
AUDITUSER = []
inpstr = []
SYSUSER = ["root","daemon","bin","sys","adm","uucp","nuucp","guest","lpd","lp","nobody","invscout","snapp","ipsec","pconsole","esaadmin","sshd"]
HEAD = ["Login","Activ_account","AdminGroup","NimeUser","Time_last_login","Technological_account"]


if platform.system() != 'AIX':
    raise Exception("This script is designed to run under the AIX operating system. If desired, it can be easily converted for any other operating system.")

##############################################################################################################################
###             Audit AIX users                                                                                            ###
##############################################################################################################################

subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)

#p = subprocess.Popen("lsuser -a account_locked time_last_login groups gecos ALL", shell=True, stdout=subprocess.PIPE)
p = subprocess.Popen("lsuser -a account_locked  groups gecos ALL", shell=True, stdout=subprocess.PIPE)
out = p.stdout.readlines()

p2 = subprocess.Popen("lsuser -a time_last_login ALL", shell=True, stdout=subprocess.PIPE)
out2 = p2.stdout.readlines()

for count, line in enumerate(out):
    # print(line.strip())
    #s = re.split(r'[ =]+', str(line.strip()))
    s = re.split(r'[ ]+', str(line.strip()), 3)
    s[0] = s[0][2:]
    s[1] = s[1][15:]
    if "false" in s[1]:
        s[1] = "True"
    else:
        s[1] = "False"
    if "admins" in s[2]:
        s[2] = "True"
    else:
        s[2] = " "
    if(len(s) == 4):
        s[3] = s[3][6:-1]
    else:
        s.append(" ")
    inpstr.append(s)


    s2 = re.split(r'[=]+', str(out2[count].strip()), 3)
    if (len(s2) == 2):
        s.append( str( datetime.fromtimestamp( int(s2[1][:-1])) ) )
    else:
        s.append("  ")


for count, s in enumerate(iter(inpstr)):
    if s[0].startswith("rb"):
        s.append("False")
    else:
        s.append("True")
    if s[0] not in SYSUSER:
        AUDITUSER.append(s)


AUDITUSER.insert(0, HEAD)

for s in AUDITUSER:
   print( ','.join(map(str, s)) )

