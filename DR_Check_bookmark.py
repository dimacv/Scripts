#!/usr/bin/python3
### DR_Check_bookmark.py

import os
import subprocess
import asyncio

class DRBookmarkChecker:
    def __init__(self):
        self.mail = '<< MAIL >>'
        self.flags = {
            'DWH': '/tmp/flag_DR_DWH_bookmark.flg',
            'MART': '/tmp/flag_DR_MART_bookmark.flg',
            'DHUB': '/tmp/flag_DR_DATAHUB_bookmark.flg'
        }
        self.file_log = '/tmp/DR_CHK_BKMRK.txt'
        self.check_bookmark_log = '/var/logs/check_bookmark_DR.txt'
        self.services_status = {
            'DWH': False,
            'MART': False,
            'DHUB': False
        }
        self.lang_env_setup()

    def lang_env_setup(self):
        """Настройка локализации среды."""
        subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)

    async def check_bookmark(self):
        """Основная логика для проверки закладки DR."""
        await self.run_snapshot_check()
        await self.process_log_file()
        await self.write_flags()
        await self.send_email()
        await self.log_results()

    async def run_snapshot_check(self):
        """Запуск команды для проверки снапшотов."""
        cmd = f'/db2home/scripts/init_db/check_snapshot_full_output.sh > {self.file_log} 2>/dev/null'
        await subprocess.call(cmd, shell=True)

    async def process_log_file(self):
        """Обработка логов из файла."""
        try:
            with open(self.file_log, 'r') as file:
                lines = file.readlines()

            str1 = []
            for line in lines:
                str1.append(line)
                if 'Storage access: LOGGED ACCESS' in line:
                    if 'DHUB_CG' in str1[-11]:
                        self.services_status['DHUB'] = True
                    elif 'DWH_CG' in str1[-11]:
                        self.services_status['DWH'] = True
                    elif 'MART_CG' in str1[-11]:
                        self.services_status['MART'] = True

        except FileNotFoundError:
            print(f"Лог файл {self.file_log} не найден!")

    async def write_flags(self):
        """Запись результатов в файлы флагов."""
        for service, flag_file in self.flags.items():
            with open(flag_file, 'w') as flag:
                flag.write('1\n' if self.services_status[service] else '0\n')

    async def send_email(self):
        """Отправка email с результатами проверки закладки."""
        message = self.generate_message()
        cmd = f'echo "{message}" | mail -s ">>>--- DR BOOKMARK TODAYS ---<<<" {self.mail}'
        await subprocess.call(cmd, shell=True)

    def generate_message(self):
        """Генерация сообщения о статусе DR закладок."""
        message = ''
        if self.services_status['DHUB']:
            message += 'OK. DHUB_CG - DR bookmark todays succeeded!\n'
        else:
            message += 'FAILED !!!.  DHUB_CG - DR bookmark todays is NOT complete!!!\n'

        if self.services_status['DWH']:
            message += 'OK. DWH_CG - DR bookmark todays succeeded!\n'
        else:
            message += 'FAILED !!!.  DWH_CG - DR bookmark todays is NOT complete!!!\n'

        if self.services_status['MART']:
            message += 'OK. MART_CG - DR bookmark todays succeeded!'
        else:
            message += 'FAILED !!!.  MART_CG - DR bookmark todays is NOT complete!!!'

        return message

    async def log_results(self):
        """Запись результатов в лог."""
        message = self.generate_message()
        with open(self.check_bookmark_log, 'w') as log_file:
            log_file.write(message)

async def main():
    checker = DRBookmarkChecker()
    await checker.check_bookmark()

if __name__ == "__main__":
    asyncio.run(main())
