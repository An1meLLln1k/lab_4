import sqlite3
import pickle
import json

# 1. Создание таблицы subitems с первичным ключом id
def create_subitems_table():
    conn = sqlite3.connect('second_task.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS subitems (
            id INTEGER PRIMARY KEY AUTOINCREMENT,  -- автоинкрементируемый первичный ключ
            name TEXT, 
            rating REAL, 
            convenience REAL, 
            security REAL, 
            functionality REAL, 
            comment TEXT
        )
    ''')
    conn.commit()
    conn.close()

# 2. Заполнение таблицы subitems из файла .pkl
def populate_subitems_from_pkl(filename):
    conn = sqlite3.connect('second_task.db')
    cursor = conn.cursor()

    with open(filename, 'rb') as file:
        data = pickle.load(file)

        # Извлекаем данные из словарей и вставляем их в таблицу
        values = [(item['name'], item['rating'], item['convenience'], item['security'], item['functionality'],
                   item['comment'])
                  for item in data]

        # Вставляем данные без указания id (он будет автоматически сгенерирован)
        cursor.executemany("INSERT INTO subitems (name, rating, convenience, security, functionality, comment) VALUES (?, ?, ?, ?, ?, ?)", values)
        conn.commit()

    conn.close()

# 3. Запрос 1: Вывести название продукта и его рейтинг
def display_product_ratings():
    conn = sqlite3.connect('second_task.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute("SELECT name, rating FROM subitems")
    results = cursor.fetchall()

    data = []
    for result in results:
        product_info = {
            "Продукт": result['name'],
            "Рейтинг": result['rating']
        }
        data.append(product_info)

    with open(r'product_ratings.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    conn.close()

# 4. Запрос 2: Вывести средние значения удобства, безопасности и функциональности
def average_ratings():
    conn = sqlite3.connect('second_task.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''SELECT AVG(convenience), AVG(security), AVG(functionality)
                      FROM subitems''')
    results = cursor.fetchone()

    average_data = {
        "Среднее удобство": results[0],
        "Средняя безопасность": results[1],
        "Средняя функциональность": results[2]
    }

    with open(r'average_ratings.json', 'w', encoding='utf-8') as file:
        json.dump(average_data, file, indent=4, ensure_ascii=False)

    conn.close()

# 5. Запрос 3: Продукты с наибольшим и наименьшим рейтингом
def find_highest_and_lowest_rated_products():
    conn = sqlite3.connect('second_task.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Продукт с наименьшим рейтингом
    cursor.execute("SELECT name, rating FROM subitems ORDER BY rating ASC LIMIT 1")
    lowest_rated = cursor.fetchone()

    # Продукт с наибольшим рейтингом
    cursor.execute("SELECT name, rating FROM subitems ORDER BY rating DESC LIMIT 1")
    highest_rated = cursor.fetchone()

    data = {
        "Продукт с наименьшим рейтингом": {
            "Продукт": lowest_rated['name'],
            "Рейтинг": lowest_rated['rating']
        },
        "Продукт с наибольшим рейтингом": {
            "Продукт": highest_rated['name'],
            "Рейтинг": highest_rated['rating']
        }
    }

    with open(r'highest_and_lowest_rated_products.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)

    conn.close()

# Выполнение всех шагов
create_subitems_table()
populate_subitems_from_pkl('subitem.pkl')
display_product_ratings()
average_ratings()
find_highest_and_lowest_rated_products()
