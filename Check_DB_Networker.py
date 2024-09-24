#!/usr/bin/python3
### Check_DB_Networker_v27.py - Paragraph <3>+<5> in the technical assignment   ###

import os
import platform
import subprocess
import datetime
import asyncio

class DBNetworkerChecker:
    def __init__(self):
        self.test_mode = platform.system() == 'Windows'
        self.time_shift = 15
        self.deph1 = 12
        self.data_today = str(datetime.date.today())
        self.this_year = self.data_today[:4]
        self.data_yesterday = str(datetime.date.today() - datetime.timedelta(days=1))
        self.cur_hour = datetime.datetime.now().hour
        self.last_year = str(datetime.date.today() - datetime.timedelta(days=365))[:4]

        # Пути для тестовой и реальной среды
        if self.test_mode:
            self.dir_init_db_log = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\log\\'
            self.flag_dwh = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\flag_DWH_backup.flg'
            self.flag_mart = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\flag_MART_backup.flg'
            self.flag_dhub = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\flag_DATAHUB_backup.flg'
            self.summary_log_name = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\DB_BAckup.log'
            self.check_db_backup_log = 'C:\\Users\\rb102656\\Desktop\\CHK_DB\\check_DB_backup.txt'
        else:
            self.dir_init_db_log = '/db2home/scripts/init_db/log/'
            self.flag_dwh = '/tmp/flag_DWH_backup.flg'
            self.flag_mart = '/tmp/flag_MART_backup.flg'
            self.flag_dhub = '/tmp/flag_DATAHUB_backup.flg'
            self.summary_log_name = '/var/logs/DB_BAckup.log'
            self.check_db_backup_log = '/var/logs/check_DB_backup.txt'

        self.dwh_ok = 'Not_Started'
        self.dhub_ok = 'Not_Started'
        self.mart_ok = 'Not_Started'
        self.file_mart_log = ''
        self.file_dwh_log = ''
        self.file_datahub_log = ''
        self.all_data = set()

        # Инициализация словарей для хранения результатов
        self.dwh_backup_dict = {}
        self.dhub_backup_dict = {}
        self.mart_backup_dict = {}

    def enumerate_files(self):
        """Метод для перечисления всех файлов в директории."""
        file_collection = []
        for dirpath, dirnames, filenames in os.walk(self.dir_init_db_log):
            for file in filenames:
                file_collection.append(file)
        return file_collection

    def load_logs(self):
        """Загрузка всех файлов журналов для анализа."""
        file_logs = self.enumerate_files()
        for file in file_logs:
            if self.this_year in file[:4] or self.last_year in file[:4]:
                self.all_data.add(file[:10])
        self.all_data = sorted(self.all_data, reverse=True)

    async def analyze_logs(self):
        """Анализ журналов резервного копирования."""
        count_data = 0
        for data in self.all_data:
            count_data += 1
            self.dwh_ok = 'False'
            self.dhub_ok = 'False'
            self.mart_ok = 'False'

            await self.check_backup_for(data)

            await self.log_results(data, count_data)

        # Отправка уведомления
        await self.send_email()

    async def check_backup_for(self, data):
        """Проверка резервных копий для заданной даты."""
        file_logs = self.enumerate_files()
        for file in file_logs:
            if data in file:
                if 'MART' in file:
                    self.file_mart_log = file
                elif 'DWH' in file:
                    self.file_dwh_log = file
                elif 'DATAHUB' in file:
                    self.file_datahub_log = file

        await self.evaluate_backup_status()

    async def evaluate_backup_status(self):
        """Оценка состояния резервных копий для MART, DWH и DATAHUB."""
        if self.file_mart_log:
            self.mart_ok = await self.check_individual_backup(self.file_mart_log, 'MART')

        if self.file_dwh_log:
            self.dwh_ok = await self.check_individual_backup(self.file_dwh_log, 'DWH')

        if self.file_datahub_log:
            self.dhub_ok = await self.check_individual_backup(self.file_datahub_log, 'DATAHUB')

    async def check_individual_backup(self, log_file, service):
        """Проверка состояния отдельной резервной копии."""
        backup_ok = 'False'
        with open(os.path.join(self.dir_init_db_log, log_file)) as file:
            log_lines = file.readlines()
            if any('+ exit 0' in line for line in log_lines):
                for i in range(self.deph1):
                    if 'NMDA backup was successful.' in log_lines[max(0, i - self.deph1):i]:
                        backup_ok = 'True'
        return backup_ok

    async def log_results(self, data, count_data):
        """Запись результатов проверки резервного копирования."""
        with open(self.flag_mart, 'w') as f_mart, \
             open(self.flag_dwh, 'w') as f_dwh, \
             open(self.flag_dhub, 'w') as f_dhub, \
             open(self.summary_log_name, 'a') as f_summ_log:

            # Логируем MART
            if self.mart_ok == 'True':
                f_mart.write('1\n')
                self.mart_backup_dict[data] = ';MART;True\n'
            else:
                f_mart.write('0\n')
                self.mart_backup_dict[data] = ';MART;False\n'

            # Логируем DWH
            if self.dwh_ok == 'True':
                f_dwh.write('1\n')
                self.dwh_backup_dict[data] = ';DWH;True\n'
            else:
                f_dwh.write('0\n')
                self.dwh_backup_dict[data] = ';DWH;False\n'

            # Логируем DATAHUB
            if self.dhub_ok == 'True':
                f_dhub.write('1\n')
                self.dhub_backup_dict[data] = ';DHUB;True\n'
            else:
                f_dhub.write('0\n')
                self.dhub_backup_dict[data] = ';DHUB;False\n'

    async def send_email(self):
        """Отправка уведомления о результатах резервного копирования."""
        message = self.generate_report()
        if not self.test_mode:
            cmd = f'echo "{message}" | mail -s "DB Backup Report" recipient@example.com'
            await subprocess.call(cmd, shell=True)

    def generate_report(self):
        """Генерация сообщения для отчета."""
        message = '[\n'
        for data in self.all_data:
            message += f'{{"DATE": "{data}", "DWH": "{self.dwh_backup_dict.get(data, "Failed")}", '
            message += f'"MART": "{self.mart_backup_dict.get(data, "Failed")}", '
            message += f'"DATAHUB": "{self.dhub_backup_dict.get(data, "Failed")}"}}\n'
        message += ']'
        return message

async def main():
    checker = DBNetworkerChecker()
    checker.load_logs()
    await checker.analyze_logs()

if __name__ == "__main__":
    asyncio.run(main())
