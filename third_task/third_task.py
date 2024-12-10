import sqlite3
import json
import pickle
import re


# 1. Создание таблицы songs
def create_songs_table():
    conn = sqlite3.connect('third_task.db')  # Используйте базу данных .db
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS songs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT,
        song TEXT,
        duration_ms INTEGER,
        year INTEGER,
        tempo REAL,
        genre TEXT,
        acousticness REAL,
        energy REAL,
        popularity INTEGER
    )''')
    conn.commit()
    conn.close()


# 2. Заполнение таблицы songs из файла .pkl
def populate_songs_from_pkl(filename):
    conn = sqlite3.connect('third_task.db')  # Используйте базу данных .db
    cursor = conn.cursor()

    with open(filename, 'rb') as file:
        data = pickle.load(file)

        # Преобразуем данные из pkl в формат для вставки
        values = [(item['artist'], item['song'], int(item['duration_ms']), int(item['year']), float(item['tempo']),
                   item['genre'], float(item['acousticness']), float(item['energy']), int(item['popularity']))
                  for item in data]

        cursor.executemany(
            "INSERT INTO songs (artist, song, duration_ms, year, tempo, genre, acousticness, energy, popularity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values)
        conn.commit()

    conn.close()


# 3. Заполнение таблицы songs из текстового файла .txt
def populate_songs_from_txt(filename):
    conn = sqlite3.connect('third_task.db')  # Используйте базу данных .db
    cursor = conn.cursor()

    with open(filename, 'r', encoding='utf-8') as txtfile:
        # Прочитаем все строки из файла и разделим их на элементы
        lines = txtfile.readlines()

        # Преобразуем данные из txt в формат для вставки
        values = []
        for line in lines:
            # Предположим, что данные разделены пробелами или табуляциями
            parts = line.strip().split()  # Разделяем по пробелам (если данные разделены табуляциями, используйте .split('\t'))

            if len(parts) == 9:  # Проверяем, что в строке 9 элементов
                try:
                    artist, song, duration_ms, year, tempo, genre, acousticness, energy, popularity = parts

                    # Преобразуем все элементы в нужные типы данных, используя try-except для обработки ошибок
                    duration_ms = int(duration_ms) if duration_ms.isdigit() else 0
                    year = int(year) if year.isdigit() else 0
                    tempo = float(tempo) if is_float(tempo) else 0.0
                    acousticness = float(acousticness) if is_float(acousticness) else 0.0
                    energy = float(energy) if is_float(energy) else 0.0
                    popularity = int(popularity) if popularity.isdigit() else 0

                    # Очистка жанра от лишних символов (удаляем скобки, апострофы и пробелы)
                    genre = clean_text(genre)

                    values.append((artist, song, duration_ms, year, tempo, genre, acousticness, energy, popularity))
                except ValueError as e:
                    print(f"Skipping invalid line: {line} due to error: {e}")

        # Вставляем все данные в таблицу
        cursor.executemany(
            "INSERT INTO songs (artist, song, duration_ms, year, tempo, genre, acousticness, energy, popularity) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values)
        conn.commit()

    conn.close()


# Функция для очистки текста (удаление скобок, кавычек, апострофов и лишних пробелов)
def clean_text(text):
    # Удаляем все символы, кроме букв, цифр, пробелов и дефисов
    text = re.sub(r'[()"\']', '', text)  # Удаляем скобки и кавычки
    text = re.sub(r'\s+', ' ', text)  # Убираем лишние пробелы между словами
    text = re.sub(r'^(\s|\()*(.*?)\s*(\)|\')*$', r'\2', text)  # Убираем скобки и пробелы в начале/конце
    text = text.strip()  # Убираем пробелы в начале и в конце
    return text


# Дополнительная проверка на число с плавающей точкой
def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False


# 4. Запрос 1: Вывод первых VAR+10 строк, отсортированных по произвольному числовому полю
def export_first_sorted_to_json(var, sort_field):
    conn = sqlite3.connect('third_task.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = f"SELECT * FROM songs ORDER BY {sort_field} LIMIT {var + 10}"
    cursor.execute(query)
    rows = cursor.fetchall()

    headers = [description[0] for description in cursor.description]
    data = [dict(zip(headers, row)) for row in rows]

    filename = "first_sorted.json"
    with open(filename, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, indent=4)

    conn.close()


# 5. Запрос 2: Вывод суммы, минимума, максимума и среднего для произвольного числового поля
def export_aggregate_results(numeric_field):
    conn = sqlite3.connect('third_task.db')
    cursor = conn.cursor()

    query = f"SELECT SUM({numeric_field}), MIN({numeric_field}), MAX({numeric_field}), AVG({numeric_field}) FROM songs"
    cursor.execute(query)
    result = cursor.fetchone()

    data = {
        "Сумма": result[0],
        "Минимум": result[1],
        "Максимум": result[2],
        "Среднее": result[3]
    }

    filename = "aggregate_results.json"
    with open(filename, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, indent=4, ensure_ascii=False)

    conn.close()

# 6. Запрос 3: Вывод частоты встречаемости для категориального поля
def export_categorical_frequency(categorical_field):
    conn = sqlite3.connect('third_task.db')
    cursor = conn.cursor()

    # Получаем частоту для каждой категории
    query = f"SELECT {categorical_field}, COUNT({categorical_field}) AS frequency FROM songs GROUP BY {categorical_field}"
    cursor.execute(query)
    rows = cursor.fetchall()

    # Очищаем категории перед записью в JSON
    data = []
    for row in rows:
        cleaned_category = clean_text(row[0]) if row[0] else "Unknown"  # Очистка текста
        data.append({"category": cleaned_category, "frequency": row[1]})

    # Записываем результат в JSON
    with open("categorical_frequency.json", "w", encoding='utf-8') as json_file:
        json.dump(data, json_file, indent=4)

    conn.close()


# 7. Запрос 4: Вывод первых VAR+15 строк, отфильтрованных по произвольному предикату, отсортированных по числовому полю
def export_filtered_sorted_to_json(var, filter_predicate, sort_field):
    conn = sqlite3.connect('third_task.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = f"SELECT * FROM songs WHERE {filter_predicate} ORDER BY {sort_field} LIMIT {var + 15}"
    cursor.execute(query)
    rows = cursor.fetchall()

    headers = [description[0] for description in cursor.description]
    data = [dict(zip(headers, row)) for row in rows]

    # Записываем результат в JSON
    filename = "filtered_sorted.json"
    with open(filename, 'w', encoding='utf-8') as outfile:
        json.dump(data, outfile, indent=4)

    conn.close()

# Выполнение всех шагов
create_songs_table()
populate_songs_from_pkl('_part_2.pkl')  # Замените на путь к вашему файлу .pkl
populate_songs_from_txt('_part_1.text')  # Замените на путь к вашему файлу .txt
export_first_sorted_to_json(34, 'duration_ms')
export_aggregate_results('tempo')
export_categorical_frequency('genre')
export_filtered_sorted_to_json(34, 'year > 2000', 'year')
