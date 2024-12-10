import sqlite3
import json
import pandas as pd
from tabulate import tabulate

def create_database():
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS baza_dannix
                      (id INTEGER, name TEXT, street TEXT, city TEXT, zipcode INTEGER, 
                       floors INTEGER, year INTEGER, parking BOOLEAN, prob_price INTEGER, views INTEGER)''')
    conn.commit()
    conn.close()

def populate_database_from_csv(filename):
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # Загрузить данные из CSV файла
    data = pd.read_csv('../1-2/item.csv', delimiter=';')

    # Вставить данные в таблицу
    sql_query = "INSERT INTO baza_dannix VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cursor.executemany(sql_query, [
        (row['id'], row['name'], row['street'], row['city'], row['zipcode'],
         row['floors'], row['year'], row['parking'], row['prob_price'], row['views']) for _, row in data.iterrows()
    ])
    conn.commit()
    conn.close()

def display_database_contents():
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM baza_dannix")
    rows = cursor.fetchall()
    headers = rows[0].keys() if rows else []
    print(tabulate(rows, headers=headers, tablefmt="fancy_grid"))
    conn.close()

def export_to_json(var):
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    num_field = 'prob_price'
    cursor.execute(f"SELECT * FROM baza_dannix ORDER BY {num_field} LIMIT {var + 10}")
    rows = cursor.fetchall()
    data = [dict(row) for row in rows]
    with open(r'output.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()

def calculate_aggregates_and_export_to_json():
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    num_field = 'prob_price'  # Выберите числовое поле для агрегирования
    cursor.execute(f"SELECT SUM({num_field}), MIN({num_field}), MAX({num_field}), AVG({num_field}) FROM baza_dannix")
    result = cursor.fetchone()
    data = {
        "Sum": result[0],
        "Min": result[1],
        "Max": result[2],
        "Average": result[3]
    }
    with open(r'aggregates_output.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()

def categorical_field_frequency_and_export_to_json(cat_field):
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute(f"SELECT {cat_field}, COUNT({cat_field}) FROM baza_dannix GROUP BY {cat_field}")
    results = cursor.fetchall()
    data = {row[cat_field]: row[f'COUNT({cat_field})'] for row in results}
    with open(r'categorical_frequency_output.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()

def export_filtered_data_to_json(var, filter_predicate):
    conn = sqlite3.connect('baza_dannix.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    num_field = 'prob_price'
    cursor.execute(f"SELECT * FROM baza_dannix WHERE {filter_predicate} ORDER BY {num_field} LIMIT {var + 10}")
    rows = cursor.fetchall()
    data = [dict(row) for row in rows]
    with open(r'filtered_output.json', 'w', encoding='utf-8') as file:
        json.dump(data, file, indent=4, ensure_ascii=False)
    conn.close()

# Выполнение шагов
create_database()
populate_database_from_csv('../1-2/item.csv')
display_database_contents()
export_to_json(34)
calculate_aggregates_and_export_to_json()
categorical_field_frequency_and_export_to_json('city')  #  по городам
export_filtered_data_to_json(34, 'prob_price > 100000000')  # фильтрация по полю prob_price
