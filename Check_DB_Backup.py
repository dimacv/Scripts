#!/usr/bin/python3
### Check_DB_Backup_v2.py - Paragraph 3 in the technical assignment  ###
import os
import datetime
import subprocess
import asyncio

class DBBackupChecker:
    def __init__(self):
        self.mail = '<< MAIL >>'
        self.deph1 = 12
        self.today = str(datetime.date.today())
        self.year = self.today[:4]
        self.yesterday = str(datetime.date.today() - datetime.timedelta(days=1))
        self.init_db_log_dir = '/db2home/scripts/init_db/log/'
        self.flags = {
            'DWH': '/tmp/flag_DWH_backup.flg',
            'MART': '/tmp/flag_MART_backup.flg',
            'DATAHUB': '/tmp/flag_DATAHUB_backup.flg'
        }
        self.summary_log_name = '/var/log/DB_BAckup.log'
        self.check_db_backup_log = '/var/logs/check_DB_backup.txt'
        self.backup_status = {
            'DWH': False,
            'MART': False,
            'DATAHUB': False
        }
        self.message = ''
        self.all_data = set()

    def enumerate_files(self):
        file_collection = []
        for dirpath, dirnames, filenames in os.walk(self.init_db_log_dir):
            for file in filenames:
                file_collection.append(file)
        return file_collection

    async def check_backups(self):
        file_logs = self.enumerate_files()
        for file in file_logs:
            if self.year in file[:4]:
                self.all_data.add(file[:10])
        self.all_data = sorted(self.all_data, reverse=True)

        # Логика проверки резервного копирования
        await self.evaluate_backups()

    async def evaluate_backups(self):
        count_data = 0
        for data in self.all_data:
            count_data += 1
            self.backup_status = {'DWH': False, 'MART': False, 'DATAHUB': False}
            self.check_backup_for(data)

            # Запись результатов
            self.log_results(data, count_data)

        # Отправка сообщения о статусе резервного копирования
        await self.send_email()

    def check_backup_for(self, data):
        file_logs = self.enumerate_files()
        for file in file_logs:
            if data in file:
                if 'MART' in file:
                    self.check_mart_backup(file)
                elif 'DWH' in file:
                    self.check_dwh_backup(file)
                elif 'DATAHUB' in file:
                    self.check_datahub_backup(file)

    def check_mart_backup(self, file):
        with open(os.path.join(self.init_db_log_dir, file)) as f:
            lines = f.readlines()
            if any('+ exit 0' in line and 'NMDA backup was successful.' in lines[max(0, i - self.deph1):i] for i, line in enumerate(lines)):
                self.backup_status['MART'] = True

    def check_dwh_backup(self, file):
        with open(os.path.join(self.init_db_log_dir, file)) as f:
            lines = f.readlines()
            if any('+ exit 0' in line and 'NMDA backup was successful.' in lines[max(0, i - self.deph1):i] for i, line in enumerate(lines)):
                self.backup_status['DWH'] = True

    def check_datahub_backup(self, file):
        with open(os.path.join(self.init_db_log_dir, file)) as f:
            lines = f.readlines()
            if any('+ exit 0' in line and 'NMDA backup was successful.' in lines[max(0, i - self.deph1):i] for i, line in enumerate(lines)):
                self.backup_status['DATAHUB'] = True

    def log_results(self, data, count_data):
        with open(self.flags['MART'], 'w') as f_mart, \
             open(self.flags['DWH'], 'w') as f_dwh, \
             open(self.flags['DATAHUB'], 'w') as f_dhub, \
             open(self.summary_log_name, 'a') as f_summ_log:

            if self.backup_status['MART']:
                if count_data <= 7:
                    self.message += f'\n------------------------------------------------\n{data}:\n+      OK        MART  -  backup succeeded!\n'
                f_mart.write('1\n')
                f_summ_log.write(f'{data};MART;True\n')
            else:
                if count_data <= 7:
                    self.message += f'\n------------------------------------------------\n{data}:\n--  FAILED   MART  -  backup is NOT succeeded!!!\n'
                f_mart.write('0\n')
                f_summ_log.write(f'{data};MART;False\n')

            if self.backup_status['DWH']:
                if count_data <= 7:
                    self.message += '+      OK        DWH   -  backup succeeded!\n'
                f_dwh.write('1\n')
                f_summ_log.write(f'{data};DWH;True\n')
            else:
                if count_data <= 7:
                    self.message += '--  FAILED   DWH   -  backup is NOT succeeded!!!\n'
                f_dwh.write('0\n')
                f_summ_log.write(f'{data};DWH;False\n')

            if self.backup_status['DATAHUB']:
                if count_data <= 7:
                    self.message += '+      OK        DHUB  -  backup succeeded!'
                f_dhub.write('1\n')
                f_summ_log.write(f'{data};DHUB;True\n')
            else:
                if count_data <= 7:
                    self.message += '--  FAILED   DHUB  -  backup is NOT succeeded!!!'
                f_dhub.write('0\n')
                f_summ_log.write(f'{data};DHUB;False\n')

    async def send_email(self):
        cmd = f'echo "Paragraph < 3 > in the technical assignment : {self.message}" | mail -s ">>>--- Paragraph < 3 > in the technical assignment - MART,DWH,DATAHUB DB BACKUP ---<<<" {self.mail}'
        await subprocess.call(cmd, shell=True)

# Основной блок запуска
if __name__ == '__main__':
    checker = DBBackupChecker()
    asyncio.run(checker.check_backups())
