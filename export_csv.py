import csv
import psycopg2

username = 'romaniuk'
password = '111'
database = 'kdramas'

TABLES = [
    'kdramas',
    'kdramas_genre',
    'actors',
    'kdramas_actor'
]

conn = psycopg2.connect(user=username, password=password, dbname=database)

with conn:
    cur = conn.cursor()

    for table_name in TABLES:
        cur.execute(f'select * from {table_name}')
        fields = [x[0] for x in cur.description]
        with open(f'{table_name}.csv', 'w', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(fields)
            for row in cur:
                writer.writerow([str(x) for x in row])