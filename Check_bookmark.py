#!/usr/bin/python3
#########  Check_bookmark.py  ##########
import os
import datetime
import subprocess
import asyncio

class BookmarkChecker:
    def __init__(self):
        self.mail = '<< MAIL >>'
        self.check_bookmark_log = '/var/logs/check_bookmark-DPF1.txt'
        self.data_today = str(datetime.date.today())
        self.str_buffer = ['', '', '']
        self.backup_bookmark_success = False
        self.flag_file = '/tmp/flag_bookmark.flg'
        self.log_file = '/db2home/scripts/DB2_RP.sh.log'
        self.lang_env_setup()

    def lang_env_setup(self):
        """Настройка локализации среды."""
        subprocess.call('LC_ALL=C;export LC_ALL;LANG=C;export LANG', shell=True)

    async def check_bookmark(self):
        """Основная логика для проверки закладки."""
        try:
            with open(self.log_file, 'r', encoding='cp1251') as file:
                lines = file.readlines()

            for line in lines:
                self.str_buffer[2] = self.str_buffer[1]
                self.str_buffer[1] = self.str_buffer[0]
                self.str_buffer[0] = line

                if self.data_today in line and 'Request for bookmark registered successfully.' in self.str_buffer[2]:
                    self.backup_bookmark_success = True

            await self.process_result()

        except FileNotFoundError:
            print(f"Лог файл {self.log_file} не найден!")

    async def process_result(self):
        """Обработка результатов проверки."""
        if self.backup_bookmark_success:
            message = f'OK. Today\'s - {self.data_today} - bookmark backup succeeded!'
            await self.send_email(message, 'OK')
            await self.write_flag('1')
        else:
            message = f'FAILED !!!. Today\'s - {self.data_today} - bookmark is NOT complete!!!'
            await self.send_email(message, 'FAILED')
            await self.write_flag('0')

        await self.log_to_file(message)
        await self.copy_log_file()

    async def send_email(self, message, status):
        """Отправка email с результатами."""
        cmd = f'echo "{message}" | mail -s "{status}. Today\'s - {self.data_today} - bookmark backup" {self.mail}'
        await subprocess.call(cmd, shell=True)

    async def write_flag(self, status):
        """Запись флага о результате."""
        with open(self.flag_file, 'w') as flag:
            flag.write(f'{status}\n')

    async def log_to_file(self, message):
        """Запись сообщений в лог файл."""
        with open(self.check_bookmark_log, 'w') as log_file:
            log_file.write(message)

    async def copy_log_file(self):
        """Копирование лог файла на удалённый сервер."""
        cmd = 'scp /var/logs/check_bookmark-DPF1.txt rsb-dbpdpfdr1:/var/logs/check_bookmark-DPF1.txt'
        await subprocess.call(cmd, shell=True)

async def main():
    checker = BookmarkChecker()
    await checker.check_bookmark()

if __name__ == "__main__":
    asyncio.run(main())
