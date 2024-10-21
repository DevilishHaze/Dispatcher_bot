
import sys

def main():
    # Получаем аргументы командной строки
    if len(sys.argv) != 4:  # ожидаем 3 параметра: userid, filename, result_path
        print("Неверное количество параметров.")
        print("Использование: TestScript.py <userid> <filename> <result_path>")
        sys.exit(1)

    # Разбор аргументов
    userid = sys.argv[1]
    filename = sys.argv[2]
    result_path = sys.argv[3]

    # Вывод параметров для проверки
    print(f"Получены параметры:")
    print(f"userid: {userid}")
    print(f"filename: {filename}")
    print(f"result_path: {result_path}")

    # Здесь может быть основная логика обработки с этими параметрами
    # Например, эмуляция обработки файла
    print(f"Обработка файла {filename} для пользователя {userid}, результат будет сохранен в {result_path}")

if __name__ == "__main__":
    main()