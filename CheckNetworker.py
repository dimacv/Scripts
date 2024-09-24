#!/usr/bin/python3
### CheckNetworker_v5.py ###

import os
import subprocess
import datetime
import asyncio

# Константы
MAIL = '<< EMAIL >>'
NSR_OUT_LOG = '/tmp/NSRINFO.txt'
DWH_RUN_LOG = '/tmp/DWH_RUN_log.txt'
MART_RUN_LOG = '/tmp/MART_RUN_log.txt'
DATAHUB_RUN_LOG = '/tmp/DATAHUB_RUN_log.txt'
DB_BACKUP_LOG_NAME = '/var/log/DB_BAckup.log'
CHECK_NETWORKER_LOG = '/var/logs/check_NETWORKER.txt'
NODES = [f'NODE{i:04}' for i in range(11)]

class BackupChecker:
    """Класс для проверки состояния резервного копирования."""

    def __init__(self):
        self.data_today = datetime.date.today().strftime('%Y%m%d')
        self.data_yesterday = (datetime.date.today() - datetime.timedelta(days=1)).strftime('%Y%m%d')
        self.all_data = set()
        self.dwh_report = [[], []]
        self.mart_report = [[], []]
        self.datahub_report = [[], []]
        self.dwh_run_now = False
        self.mart_run_now = False
        self.datahub_run_now = False

    async def execute_command(self, command):
        """Асинхронное выполнение команды."""
        process = await asyncio.create_subprocess_shell(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if stderr:
            raise Exception(f"Ошибка выполнения команды: {stderr.decode().strip()}")
        return stdout.decode().splitlines()

    async def check_networker(self):
        """Основной метод для проверки Networker."""
        await self.run_backup_commands()
        await self.parse_backup_logs()

    async def run_backup_commands(self):
        """Выполнение команд для получения информации о резервном копировании."""
        subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)
        commands = [
            f'nsrinfo -v -s << ADDRES >> -n db2 -X all rsb-dbpdpfdr{i} > {NSR_OUT_LOG}' for i in range(1, 4)
        ]
        await asyncio.gather(*(self.execute_command(cmd) for cmd in commands))

        run_commands = [
            f'sudo -iu db2khdci /db2home/db2khdci/sqllib/adm/db2pd -util -dbp all | grep bytes > {DWH_RUN_LOG}',
            f'sudo -iu db2marti /db2home/db2khdci/sqllib/adm/db2pd -util -dbp all | grep bytes > {MART_RUN_LOG}',
            f'sudo -iu db2dhpi /db2home/db2khdci/sqllib/adm/db2pd -util -dbp all | grep bytes > {DATAHUB_RUN_LOG}',
        ]
        await asyncio.gather(*(self.execute_command(cmd) for cmd in run_commands))

    async def parse_backup_logs(self):
        """Парсинг логов резервного копирования."""
        with open(DB_BACKUP_LOG_NAME, 'r') as f:
            db_backup_log_lines = f.readlines()

        with open(NSR_OUT_LOG) as f:
            inp = f.readlines()
            for line in inp:
                pos = line.find('/DB_BACKUP.')
                if pos != -1:
                    self.all_data.add(line[pos + 11:pos + 19])
            self.all_data = sorted(self.all_data, reverse=True)

        if self.all_data and self.all_data[0] == self.data_today:
            self.all_data.pop(0)

        for entry in self.all_data:
            self.dwh_report[0].append(entry)
            self.mart_report[0].append(entry)
            self.datahub_report[0].append(entry)

        await self.check_nodes(db_backup_log_lines)

    async def check_nodes(self, db_backup_log_lines):
        """Проверка состояния узлов резервного копирования."""
        self.dwh_run_now = await self.is_running(DWH_RUN_LOG)
        self.mart_run_now = await self.is_running(MART_RUN_LOG)
        self.datahub_run_now = await self.is_running(DATAHUB_RUN_LOG)

        await self.check_report(self.dwh_report, 'DWH', db_backup_log_lines)
        await self.check_report(self.mart_report, 'MART', db_backup_log_lines)
        await self.check_report(self.datahub_report, 'DATAHUB', db_backup_log_lines)

    async def is_running(self, log_file):
        """Проверка, запущен ли процесс на основе логов."""
        with open(log_file) as f:
            return any('bytes' in line for line in f.readlines())

    async def check_report(self, report, service_name, db_backup_log_lines):
        """Проверка отчетов для сервисов."""
        for entry in report[0]:
            report[1].append('FAILED')

        for count, data_ind in enumerate(report[0]):
            nodes_status = [False] * len(NODES)
            for line in report[0]:
                if data_ind in line and service_name in line:
                    report[1][count] = 'FAILED'
                    for idx, node in enumerate(NODES):
                        if node in line:
                            nodes_status[idx] = True

            if all(nodes_status):
                report[1][count] = 'Ok'
                for log_line in db_backup_log_lines:
                    strdt1 = f'{data_ind[:4]}-{data_ind[4:6]}-{data_ind[6:8]}'
                    if strdt1 in log_line and service_name in log_line and 'False' in log_line:
                        report[1][count] = 'FAILED'

    def generate_message(self):
        """Генерация сообщения о статусе резервного копирования."""
        message = ""
        for count in range(len(self.all_data)):
            date_str = self.dwh_report[0][count]
            date_formatted = f'{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}'
            message += f"{date_formatted} : \nDWH  -   {self.dwh_report[1][count]}"
            message += f"\nMART  -   {self.mart_report[1][count]}"
            message += f"\nDATAHUB  -   {self.datahub_report[1][count]}\n---------------------------\n"
        return message

    async def notify(self):
        """Отправка уведомления о статусе резервного копирования."""
        message = self.generate_message()
        print(message)  # Тест в консоли
        cmd = f'echo "Paragraph < 5 > in the technical assignment:\n  BACKUPS ARE FOUND STORED IN NETWORKER : \n---------------------------\n{message}" | mail -s ">>>---  BACKUPS ARE FOUND STORED IN NETWORKER: ---<<<" {MAIL}'
        await self.execute_command(cmd)

        with open(CHECK_NETWORKER_LOG, 'w') as f:
            f.write(message)

async def main():
    """Главная асинхронная функция."""
    checker = BackupChecker()
    await checker.check_networker()
    await checker.notify()

# Запуск главной функции
if __name__ == "__main__":
    asyncio.run(main())
