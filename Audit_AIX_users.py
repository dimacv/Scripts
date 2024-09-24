#!/usr/bin/python3
###  AuditAIXUsers.py   ###

from __future__ import print_function
import os
import platform
import subprocess
import datetime
import re
import asyncio

# Константы
OUTSEP = " ; "
SYSUSER = ["root", "daemon", "bin", "sys", "adm", "uucp", "nuucp", "guest", "lpd", "lp", "nobody", "invscout", "snapp", "ipsec", "pconsole", "esaadmin", "sshd"]
HEAD = ["Login", "Activ_account", "AdminGroup", "NimeUser", "Time_last_login", "Technological_account"]

# Исключение для неподдерживаемых ОС
if platform.system() != 'AIX':
    raise Exception("Этот скрипт предназначен для работы в операционной системе AIX.")

class AuditAIXUsers:
    """Класс для аудита пользователей AIX."""

    def __init__(self):
        self.audit_users = []  # Список для хранения пользователей
        self.input_strings = []  # Входные данные

    async def execute_command(self, command):
        """Асинхронное выполнение команды."""
        process = await asyncio.create_subprocess_shell(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = await process.communicate()
        if stderr:
            raise Exception(f"Ошибка выполнения команды: {stderr.decode().strip()}")
        return stdout.decode().splitlines()

    async def audit_users(self):
        """Основной метод для аудита пользователей."""
        # Выполнение команд для получения информации о пользователях
        user_info = await self.execute_command("lsuser -a account_locked groups gecos ALL")
        last_login_info = await self.execute_command("lsuser -a time_last_login ALL")

        for count, line in enumerate(user_info):
            # Обработка информации о пользователе
            user_data = re.split(r'[ ]+', line.strip(), 3)
            user_data[0] = user_data[0][2:]
            user_data[1] = "True" if "false" in user_data[1] else "False"
            user_data[2] = "True" if "admins" in user_data[2] else " "
            user_data.append(user_data[3][6:-1] if len(user_data) == 4 else " ")

            # Получение времени последнего входа
            last_login_data = re.split(r'[=]+', last_login_info[count].strip(), 3)
            user_data.append(str(datetime.datetime.fromtimestamp(int(last_login_data[1][:-1]))) if len(last_login_data) == 2 else " ")

            self.input_strings.append(user_data)

        for user in self.input_strings:
            user.append("False" if user[0].startswith("rb") else "True")
            if user[0] not in SYSUSER:
                self.audit_users.append(user)

        # Добавление заголовка
        self.audit_users.insert(0, HEAD)

    def display_audit_results(self):
        """Метод для отображения результатов аудита."""
        for user in self.audit_users:
            print(','.join(map(str, user)))

async def main():
    """Главная асинхронная функция."""
    audit = AuditAIXUsers()
    await audit.audit_users()
    audit.display_audit_results()

# Запуск главной функции
if __name__ == "__main__":
    asyncio.run(main())
