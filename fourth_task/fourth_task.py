import sqlite3
import json
import pickle

# 1. Создание таблицы products с добавлением счётчика обновлений
def create_products_table(cursor):
    cursor.execute(''' 
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        quantity INTEGER,
        category TEXT,
        fromCity TEXT,
        isAvailable BOOLEAN,
        views INTEGER,
        update_counter INTEGER DEFAULT 0  -- Счётчик обновлений
    )''')

# 2. Обработка данных из .text файла (товары)
def insert_products_from_text(cursor, products_data):
    products = products_data.split('=====\n')  # Разделим товары по разделителю

    all_products = []
    for product in products:
        # Убираем лишние пробелы по краям строки и разбиваем на строки по '\n'
        product_info = product.strip().split('\n')

        # Проверяем, что строка имеет хотя бы 6 элементов
        if len(product_info) >= 6:
            try:
                # Чтение данных с проверками на наличие
                name = product_info[0].split('::')[1].strip() if len(product_info[0].split('::')) > 1 else 'Unknown'
                price = round(float(product_info[1].split('::')[1].strip()), 2) if len(
                    product_info[1].split('::')) > 1 else 0.0  # Округление до 2 знаков
                quantity = int(product_info[2].split('::')[1].strip()) if len(product_info[2].split('::')) > 1 else 0
                category = product_info[3].split('::')[1].strip() if len(product_info[3].split('::')) > 1 else 'Unknown'
                fromCity = product_info[4].split('::')[1].strip() if len(product_info[4].split('::')) > 1 else 'Unknown'
                isAvailable = product_info[5].split('::')[1].strip() == 'True' if len(
                    product_info[5].split('::')) > 1 else False

                # Проверка для views, если элемент не существует, ставим 0
                views = 0  # Значение по умолчанию
                if len(product_info) > 6:
                    try:
                        views = int(product_info[6].split('::')[1].strip())
                    except ValueError:
                        views = 0  # Если не удалось преобразовать в число

                # Добавляем товар в список
                all_products.append((name, price, quantity, category, fromCity, isAvailable, views))
            except IndexError:
                print(f"Ошибка в строке товара (недостаточно данных): {product}")
            except ValueError as ve:
                print(f"Ошибка в формате данных для товара: {product}, ошибка: {ve}")

    # Вставляем данные в таблицу, если они корректные
    cursor.executemany(
        "INSERT INTO products (name, price, quantity, category, fromCity, isAvailable, views) VALUES (?, ?, ?, ?, ?, ?, ?)",
        all_products)

# 3. Обработка изменений из .pkl файла
def apply_changes(cursor, changes):
    for change in changes:
        name = change['name']
        method = change['method']
        param = change['param']

        if method == 'available':
            cursor.execute("UPDATE products SET isAvailable = ?, update_counter = update_counter + 1 WHERE name = ? AND isAvailable != ?",
                           (param, name, param))
        elif method == 'price_percent':
            cursor.execute("UPDATE products SET price = price * (1 + ?), update_counter = update_counter + 1 WHERE name = ? AND price > 0",
                           (param, name))
        elif method == 'price_abs':
            cursor.execute("UPDATE products SET price = price + ?, update_counter = update_counter + 1 WHERE name = ? AND price + ? >= 0",
                           (param, name, param))
        elif method == 'quantity_add':
            cursor.execute("UPDATE products SET quantity = quantity + ?, update_counter = update_counter + 1 WHERE name = ? AND quantity + ? >= 0",
                           (param, name, param))
        elif method == 'quantity_sub':
            cursor.execute("UPDATE products SET quantity = quantity - ?, update_counter = update_counter + 1 WHERE name = ? AND quantity - ? >= 0",
                           (param, name, param))
        elif method == 'remove':
            cursor.execute("DELETE FROM products WHERE name = ?", (name,))

# 4. Запрос: Топ-10 самых обновляемых товаров
def query_top_updated_products(cursor):
    cursor.execute("SELECT name, update_counter FROM products ORDER BY update_counter DESC LIMIT 10")
    top_updated_products = cursor.fetchall()
    formatted_top_updated_products = [{"Товар": product[0], "Количество обновлений": product[1]} for product in top_updated_products]
    return formatted_top_updated_products

# 5. Запрос: Анализ цен товаров по категориям
def query_price_analysis(cursor):
    cursor.execute('''SELECT category, 
     SUM(price) AS total_price, 
     MIN(price) AS min_price, 
     MAX(price) AS max_price, 
     AVG(price) AS avg_price, 
     COUNT(*) AS product_count 
     FROM products 
     GROUP BY category''')
    price_analysis = cursor.fetchall()
    formatted_price_analysis = [
        {
            "Категория": category[0],
            "Сумма цен товаров": category[1],
            "Минимальная цена": category[2],
            "Максимальная цена": category[3],
            "Средняя цена": category[4],
            "Количество товаров в категории": category[5]
        } for category in price_analysis]

    return formatted_price_analysis

# 6. Запрос: Анализ остатков товаров по категориям
def query_quantity_analysis(cursor):
    cursor.execute('''SELECT category, 
     SUM(quantity) AS total_quantity, 
     MIN(quantity) AS min_quantity, 
     MAX(quantity) AS max_quantity, 
     AVG(quantity) AS avg_quantity, 
     COUNT(*) AS product_count 
     FROM products 
     GROUP BY category''')
    quantity_analysis = cursor.fetchall()
    formatted_quantity_analysis = [
        {
            "Категория": category[0],
            "Сумма остатков товаров": category[1],
            "Минимальный остаток": category[2],
            "Максимальный остаток": category[3],
            "Средний остаток": category[4],
            "Количество товаров в категории": category[5]
        } for category in quantity_analysis]
    return formatted_quantity_analysis

# 7. Запрос: Произвольный запрос
def query_custom(cursor):
    # Запрос с фильтрацией, агрегацией и сортировкой
    cursor.execute('''
        SELECT category, 
               MAX(price) AS max_price, 
               MIN(price) AS min_price, 
               AVG(price) AS avg_price,
               MAX(quantity) AS max_quantity, 
               COUNT(*) AS product_count
        FROM products
        WHERE quantity > 10 AND isAvailable = 1
        GROUP BY category
        ORDER BY avg_price DESC, category ASC
    ''')
    rows = cursor.fetchall()
    return rows

# Основная функция
def main():
    conn = sqlite3.connect('fourth_task.db')
    cursor = conn.cursor()

    # Создание таблиц
    create_products_table(cursor)

    # Чтение и вставка данных из текстового файла
    with open('_product_data.text', 'r', encoding='utf-8') as product_file:
        products_data = product_file.read()
    insert_products_from_text(cursor, products_data)

    # Применение изменений из .pkl файла
    with open('_update_data.pkl', 'rb') as update_file:
        changes = pickle.load(update_file)
    apply_changes(cursor, changes)

    conn.commit()

    # Запросы и запись в файлы
    with open('top_updated_products.json', 'w', encoding='utf-8') as file:
        json.dump(query_top_updated_products(cursor), file, ensure_ascii=False, indent=4)

    with open('price_analysis.json', 'w', encoding='utf-8') as file:
        json.dump(query_price_analysis(cursor), file, ensure_ascii=False, indent=4)

    with open('quantity_analysis.json', 'w', encoding='utf-8') as file:
        json.dump(query_quantity_analysis(cursor), file, ensure_ascii=False, indent=4)

    # Выполнение произвольного запроса и запись в JSON
    custom_query_result = query_custom(cursor)
    with open('custom_query_result.json', 'w', encoding='utf-8') as file:
        json.dump(custom_query_result, file, ensure_ascii=False, indent=4)

    conn.close()

if __name__ == "__main__":
    main()

