import time
import os
import subprocess

import connectionDB

folder_path = 'C:\\Users\\dlo_4\\Desktop\\Loader' #поставить папку, где хранятся файлы для загрузки


max_workers = 10

"""Поиск файлов в ожидании запросом из БД"""


def looking_for_files(connection):
    cursor = connection.cursor()

    try:
        cursor.execute("SELECT * "
                       "FROM flk.filer.workfiles "
                       "WHERE loadstatus = 20  "
                       "and Priority >0 "
                       "ORDER BY priority DESC")
        return cursor.fetchall()
    finally:
        cursor.close()


"""Проверка, есть ли этот файл в папке"""


def check_files_in_folder(files, folder_path):
    missing_files = []

    for file in files:
        filename = file[11].strip()
        file_path = os.path.join(folder_path, filename)

        if not os.path.exists(file_path):
            missing_files.append(file[0])
    return missing_files

"""Обновление статуса отсутствующего файла"""


def update_missing_file_status(connection, filename):
    cursor = connection.cursor()

    try:
        cursor.execute("UPDATE flk.filer.workfiles "
                       "SET loadstatus = -23 "
                       "WHERE filename = %s", (filename,))
        connection.commit()

        # Перемещаем print внутрь функции после успешного обновления
        print(f"Файл {filename} обновлён с статусом -23.")

    except Exception as e:
        print(f"Ошибка обновления записи для {filename}: {e}")
        connection.rollback()
    finally:
        cursor.close()

"""Подсчет активных воркеров"""


def count_active_workers(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT COUNT(*) "
                       "FROM flk.filer.workfiles "
                       "WHERE loadstatus = 22")
        return cursor.fetchone()[0]
    finally:
        cursor.close()


"""Основной цикл работы бота"""


def main():
    connection = connectionDB.create_connection("flk", "progelmed", "progelmed", "192.168.0.149", "5432")

    while True:
        # Поиск файлов в статусе ожидания (loadstatus = 20)
        files_in_waiting = looking_for_files(connection)

        if files_in_waiting:
            # Проверка наличия файлов в папке
            missing_files = check_files_in_folder(files_in_waiting,folder_path)

            # Обновление статуса для отсутствующих файлов
            for file_id in missing_files:
                filename = next(file[11] for file in files_in_waiting if file[0] == file_id)
                update_missing_file_status(connection, filename)


            # Подсчет активных воркеров (loadstatus = 22)
            active_workers = count_active_workers(connection)

            if active_workers < max_workers:
                for file in files_in_waiting:
                    if len(file) > 21:  # Проверка, что в кортеже достаточно элементов
                        userid = str(file[12])  # Преобразуем в строку
                        filename = str(file[11])  # Преобразуем в строку
                        result_path = str(file[21])  # Преобразуем в строку

                        # Запуск воркера с параметрами
                        worker_process = subprocess.Popen(
                            ['python', 'D:/PyProject/Dispatcher/TestScript.py', userid, filename, result_path])

                        while worker_process.poll() is None:
                            print(
                                f"Процесс обрабатывает файл {filename} для пользователя {userid}, ожидаем завершения.")
                            time.sleep(5)
                    else:
                        print("Некорректная запись файла, недостаточно данных в файле:", file)
            else:
                print("Все воркеры заняты, ждем освобождения ресурсов.")

            time.sleep(60)


if __name__ == "__main__":
    main()
