import json
import psycopg2

username = 'romaniuk'
password = '111'
database = 'kdramas'
host = 'localhost'
port = '5432'

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

data = {}
with conn:

    cur = conn.cursor()

    for table in ('kdramas', 'kdramas_genre', 'actors', 'kdramas_actor'):
        cur.execute('select * from ' + table)
        rows = []
        fields = [x[0] for x in cur.description]
        for row in cur:
            rows.append(dict(zip(fields, row)))
        data[table] = rows


    with open('all_tables_data.json', 'w', encoding='utf-8') as outf:
        json.dump(data, outf, default=str)
