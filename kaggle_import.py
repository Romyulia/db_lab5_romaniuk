import csv
import decimal
import psycopg2

username = 'romaniuk'
password = '111'
database = 'kdramas'

INPUT_CSV_FILE = 'series.csv'
INPUT_CSV_FILE2 = 'genres.csv'

query_00 = '''
DROP TABLE IF EXISTS kdramas
'''

query_0 = '''
CREATE TABLE kdramas
(
  kdrama_id INT NOT NULL,
  kdrama_name VARCHAR(50) NOT NULL,
  number_of_episodes INT NOT NULL,
  airing_date DATE NOT NULL,
  episode_run_time INT NOT NULL,
  PRIMARY KEY (kdrama_id)
);
'''

query_01 = '''
DELETE FROM kdramas
'''

query_02 = '''
INSERT INTO kdramas (kdrama_id, kdrama_name, number_of_episodes, airing_date, episode_run_time)
VALUES
(%s, %s, %s, %s, %s),
(%s, %s, %s, %s, %s),
(%s, %s, %s, %s, %s),
(%s, %s, %s, %s, %s),
(%s, %s, %s, %s, %s),
(%s, %s, %s, %s, %s)
'''

query_10 = '''
DROP TABLE IF EXISTS kdramas_genre
'''

query_1 = '''
CREATE TABLE kdramas_genre
(
  genre_name VARCHAR(20) NOT NULL,
  kdrama_id INT NOT NULL,
  PRIMARY KEY (genre_name, kdrama_id),
  FOREIGN KEY (kdrama_id) REFERENCES kdramas(kdrama_id)
);
'''

query_11 = '''
DELETE FROM kdramas_genre
'''

query_12 = '''
INSERT INTO kdramas_genre (genre_name, kdrama_id)
VALUES
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s),
(%s, %s)
'''

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()
    cur.execute(query_10)
    cur.execute(query_00)
    cur.execute(query_0)
    cur.execute(query_01)

    kdramas_data_list = []
    genres_data_list = []
    with open(INPUT_CSV_FILE, 'r', encoding="utf8") as inf:

        csv_read = csv.reader(inf)
        next(csv_read)

        for row in csv_read:
            kdramas_data_list.append(row)

    with open(INPUT_CSV_FILE2, 'r', encoding="utf8") as file:
        csv2_read = csv.reader(file)
        next(csv2_read)
        for row in csv2_read:
            genres_data_list.append(row[1])

    cur.execute(query_02, (kdramas_data_list[0][0], kdramas_data_list[0][1], decimal.Decimal(kdramas_data_list[0][11]), kdramas_data_list[0][4], decimal.Decimal(kdramas_data_list[0][12]),
                          kdramas_data_list[1][0], kdramas_data_list[1][1], decimal.Decimal(kdramas_data_list[1][11]), kdramas_data_list[1][4], decimal.Decimal(kdramas_data_list[1][12]),
                          kdramas_data_list[2][0], kdramas_data_list[2][1], decimal.Decimal(kdramas_data_list[2][11]), kdramas_data_list[2][4], decimal.Decimal(kdramas_data_list[2][12]),
                          kdramas_data_list[3][0], kdramas_data_list[3][1], decimal.Decimal(kdramas_data_list[3][11]), kdramas_data_list[3][4], decimal.Decimal(kdramas_data_list[3][12]),
                          kdramas_data_list[4][0], kdramas_data_list[4][1], decimal.Decimal(kdramas_data_list[4][11]), kdramas_data_list[4][4], decimal.Decimal(kdramas_data_list[4][12]),
                          kdramas_data_list[5][0], kdramas_data_list[5][1], decimal.Decimal(kdramas_data_list[5][11]), kdramas_data_list[5][4], decimal.Decimal(kdramas_data_list[5][12])))

    cur.execute(query_10)
    cur.execute(query_1)
    cur.execute(query_11)

    cur.execute(query_12, (genres_data_list[0], kdramas_data_list[0][0],
                           genres_data_list[1], kdramas_data_list[0][0],
                           genres_data_list[2], kdramas_data_list[0][0],
                           genres_data_list[3], kdramas_data_list[1][0],
                           genres_data_list[0], kdramas_data_list[1][0],
                           genres_data_list[4], kdramas_data_list[1][0],
                           genres_data_list[5], kdramas_data_list[2][0],
                           genres_data_list[0], kdramas_data_list[2][0],
                           genres_data_list[3], kdramas_data_list[3][0],
                           genres_data_list[2], kdramas_data_list[3][0],
                           genres_data_list[0], kdramas_data_list[3][0],
                           genres_data_list[0], kdramas_data_list[4][0],
                           genres_data_list[2], kdramas_data_list[4][0],
                           genres_data_list[6], kdramas_data_list[4][0],
                           genres_data_list[0], kdramas_data_list[5][0],
                           genres_data_list[5], kdramas_data_list[5][0],
                           genres_data_list[2], kdramas_data_list[5][0],
                           genres_data_list[4], kdramas_data_list[5][0]))

    conn.commit()