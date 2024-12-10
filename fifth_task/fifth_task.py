import sqlite3
import json
import csv


# 1. Создание таблиц
def create_tables(cursor):
    # Удаляем старые таблицы, если они существуют
    cursor.execute('''DROP TABLE IF EXISTS Movies_and_Shows''')
    cursor.execute('''DROP TABLE IF EXISTS Genres''')
    cursor.execute('''DROP TABLE IF EXISTS Countries''')
    cursor.execute('''DROP TABLE IF EXISTS Reviews''')

    # Таблица жанров
    cursor.execute('''CREATE TABLE IF NOT EXISTS Genres (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        genre_name TEXT)''')

    # Таблица стран
    cursor.execute('''CREATE TABLE IF NOT EXISTS Countries (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        country_name TEXT)''')

    # Таблица фильмов и шоу
    cursor.execute('''CREATE TABLE IF NOT EXISTS Movies_and_Shows (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        title TEXT,
                        type TEXT,
                        release_year INTEGER,
                        rating TEXT,
                        duration TEXT,
                        genre_id INTEGER,
                        country_id INTEGER,
                        views INTEGER,
                        FOREIGN KEY (genre_id) REFERENCES Genres(id),
                        FOREIGN KEY (country_id) REFERENCES Countries(id))''')


# 2. Загрузка данных из CSV в таблицы
def load_movies_from_csv(cursor, csv_file):
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)

        # Проверим, что заголовки корректно распознались
        headers = reader.fieldnames
        print(f"Заголовки в CSV: {headers}")  # Это для отладки

        for row in reader:
            # Прочитаем данные из строки
            title = row.get('Title', '').strip()  # Используем .get() для защиты от KeyError
            type = row.get('Type', '').strip()
            genre = row.get('Genre', '').strip()
            release_year = row.get('Release Year', '').strip()
            rating = row.get('Rating', '').strip()
            duration = row.get('Duration', '').strip()
            country = row.get('Country', '').strip()

            if not title or not genre or not country:  # Пропустим строки с неполными данными
                continue

            # Вставляем данные в таблицу Genres
            cursor.execute('''INSERT OR IGNORE INTO Genres (genre_name) VALUES (?)''',
                           (genre,))
            cursor.execute('''INSERT OR IGNORE INTO Countries (country_name) VALUES (?)''',
                           (country,))

            # Получаем ID жанра и страны
            cursor.execute('''SELECT id FROM Genres WHERE genre_name = ?''', (genre,))
            genre_id = cursor.fetchone()[0]
            cursor.execute('''SELECT id FROM Countries WHERE country_name = ?''', (country,))
            country_id = cursor.fetchone()[0]

            # Вставляем данные в таблицу Movies_and_Shows
            cursor.execute('''INSERT INTO Movies_and_Shows (title, type, release_year, rating, duration, genre_id, country_id, views)
                              VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                           (title, type, int(release_year) if release_year.isdigit() else None, rating, duration,
                            genre_id, country_id, 0))


# 3. Загрузка данных из JSON
def load_movies_from_json(cursor, json_file):
    with open(json_file, 'r', encoding='utf-8') as file:
        movies = json.load(file)

    for movie in movies:
        title = movie.get('Title', '').strip()
        type = movie.get('Type', '').strip()
        genre = movie.get('Genre', '').strip()
        release_year = movie.get('Release Year', '').strip()
        rating = movie.get('Rating', '').strip()
        duration = movie.get('Duration', '').strip()
        country = movie.get('Country', '').strip()

        if not title or not genre or not country:
            continue

        # Вставляем данные в таблицу Genres и Countries
        cursor.execute('''INSERT OR IGNORE INTO Genres (genre_name) VALUES (?)''', (genre,))
        cursor.execute('''INSERT OR IGNORE INTO Countries (country_name) VALUES (?)''', (country,))

        # Получаем ID жанра и страны
        cursor.execute('''SELECT id FROM Genres WHERE genre_name = ?''', (genre,))
        genre_id = cursor.fetchone()[0]
        cursor.execute('''SELECT id FROM Countries WHERE country_name = ?''', (country,))
        country_id = cursor.fetchone()[0]

        # Вставляем данные в таблицу Movies_and_Shows
        cursor.execute('''INSERT INTO Movies_and_Shows (title, type, release_year, rating, duration, genre_id, country_id, views)
                          VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                       (title, type, int(release_year) if release_year.isdigit() else None, rating, duration, genre_id,
                        country_id, 0))


# 4. Запрос: Топ-10 самых обновляемых фильмов/шоу (по просмотрам)
def top_10_movies_by_views(cursor):
    cursor.execute('''SELECT title, views FROM Movies_and_Shows ORDER BY views DESC LIMIT 10''')
    rows = cursor.fetchall()
    return [{"Title": row[0], "Views": row[1]} for row in rows]


# 5. Запрос: Средняя оценка по жанрам
def average_rating_by_genre(cursor):
    cursor.execute('''SELECT genre_name, AVG(rating) as avg_rating 
                      FROM Movies_and_Shows 
                      JOIN Genres ON Movies_and_Shows.genre_id = Genres.id 
                      GROUP BY genre_name''')
    rows = cursor.fetchall()
    return [{"Genre": row[0], "Average Rating": row[1]} for row in rows]


# 6. Запрос: Количество фильмов по странам
def count_movies_by_country(cursor):
    cursor.execute('''SELECT country_name, COUNT(*) as movie_count 
                      FROM Movies_and_Shows 
                      JOIN Countries ON Movies_and_Shows.country_id = Countries.id
                      GROUP BY country_name''')
    rows = cursor.fetchall()
    return [{"Country": row[0], "Movie Count": row[1]} for row in rows]


# 7. Запрос: Фильмы с рейтингом выше 8
def movies_with_rating_above_8(cursor):
    cursor.execute('''SELECT title, rating FROM Movies_and_Shows WHERE rating > "8"''')
    rows = cursor.fetchall()
    return [{"Title": row[0], "Rating": row[1]} for row in rows]


# 8. Произвольный запрос: Все фильмы/шоу с рейтингом выше 8
def custom_query(cursor):
    cursor.execute('''SELECT title, rating FROM Movies_and_Shows WHERE rating > "8"''')
    rows = cursor.fetchall()
    return [{"Title": row[0], "Rating": row[1]} for row in rows]


# 9. Группировка по типу фильма и подсчет среднего рейтинга для каждого типа
def avg_rating_by_type(cursor):
    cursor.execute('''SELECT type, AVG(rating) as avg_rating 
                      FROM Movies_and_Shows 
                      GROUP BY type''')
    rows = cursor.fetchall()
    return [{"Type": row[0], "Average Rating": row[1]} for row in rows]


# 10. Вывод информации о фильмах с минимальной продолжительностью (например, 1 сезон или короткие фильмы)
def short_movies(cursor):
    cursor.execute('''SELECT title, duration FROM Movies_and_Shows WHERE duration LIKE "1%" OR duration LIKE "%Seasons"''')
    rows = cursor.fetchall()
    return [{"Title": row[0], "Duration": row[1]} for row in rows]


# Основная функция
def main():
    conn = sqlite3.connect('movies_and_shows.db')
    cursor = conn.cursor()

    # Создание таблиц
    create_tables(cursor)

    # Загрузка данных
    load_movies_from_csv(cursor, 'cleaned_first_part.csv')  # Путь к вашему CSV файлу
    load_movies_from_json(cursor, 'cleaned_second_part.json')  # Путь к вашему JSON файлу

    # Сохранение данных в базу
    conn.commit()

    # Запросы и сохранение в JSON
    with open('top_updated_movies.json', 'w', encoding='utf-8') as file:
        json.dump(top_10_movies_by_views(cursor), file, ensure_ascii=False, indent=4)

    with open('average_rating_by_genre.json', 'w', encoding='utf-8') as file:
        json.dump(average_rating_by_genre(cursor), file, ensure_ascii=False, indent=4)

    with open('count_movies_by_country.json', 'w', encoding='utf-8') as file:
        json.dump(count_movies_by_country(cursor), file, ensure_ascii=False, indent=4)

    with open('movies_with_rating_above_8.json', 'w', encoding='utf-8') as file:
        json.dump(movies_with_rating_above_8(cursor), file, ensure_ascii=False, indent=4)

    with open('avg_rating_by_type.json', 'w', encoding='utf-8') as file:
        json.dump(avg_rating_by_type(cursor), file, ensure_ascii=False, indent=4)

    with open('short_movies.json', 'w', encoding='utf-8') as file:
        json.dump(short_movies(cursor), file, ensure_ascii=False, indent=4)

    # Закрытие соединения с базой данных
    conn.close()


if __name__ == "__main__":
    main()

