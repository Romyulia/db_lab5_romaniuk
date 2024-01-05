import psycopg2
import matplotlib.pyplot as plt

username = 'romaniuk'
password = '111'
database = 'kdramas'
host = 'localhost'
port = '5432'

query_1 = '''
create view genre_kdrama_quantity as
select genre_name, count(genre_name)
from kdramas_genre
group by genre_name;
'''
query_2 = '''
create view role_gender as
select gender, count(gender)
from kdramas join kdramas_actor
on kdramas.kdrama_id = kdramas_actor.kdrama_id
join actors
on kdramas_actor.actor_id = actors.actor_id
group by gender;
'''
query_3 = '''
create view duration_of_kdramas as
select kdrama_name, number_of_episodes * episode_run_time as duration
from kdramas
where airing_date >= '2022-01-01';
'''

conn = psycopg2.connect(user=username, password=password, dbname=database, host=host, port=port)

with conn:
    cur = conn.cursor()

    cur.execute('drop view if exists genre_kdrama_quantity')
    cur.execute(query_1)
    cur.execute('select * from genre_kdrama_quantity')
    genre = []
    total = []

    for row in cur:
        genre.append(row[0])
        total.append(row[1])

    x_range = range(len(genre))

    figure, (bar_ax, pie_ax, bar2_ax) = plt.subplots(1, 3)
    bar = bar_ax.bar(x_range, total, label='Total')
    bar_ax.bar_label(bar, label_type='center')
    bar_ax.set_xticks(x_range)
    bar_ax.set_xticklabels(genre)
    bar_ax.set_xlabel('Genre')
    bar_ax.set_ylabel('Times')
    bar_ax.set_title('Кількість дорам відповідно до жанрів')

    cur.execute('drop view if exists role_gender')
    cur.execute(query_2)
    cur.execute('select * from role_gender')
    gender = []
    quantity = []

    for row in cur:
        if row[0] == 1:
            gender.append('female')
        else:
            gender.append('male')
        quantity.append(row[1])

    pie_ax.pie(quantity, labels=gender, autopct='%1.1f%%')
    pie_ax.set_title('Відсоток жінок акторок, які зіграли в дорамах до чоловіків')

    cur.execute('drop view if exists duration_of_kdramas')
    cur.execute(query_3)
    cur.execute('select * from duration_of_kdramas')
    kdrama = []
    time = []

    for row in cur:
        kdrama.append(row[0])
        time.append(row[1])

    x2_range = range(len(kdrama))

    bar2 = bar2_ax.bar(x2_range, time, label='Duration')
    bar2_ax.bar_label(bar2, label_type='center')
    bar2_ax.set_xticks(x2_range)
    bar2_ax.set_xticklabels(kdrama)
    bar2_ax.set_xlabel('Kdrama name')
    bar2_ax.set_ylabel('Duration')
    bar2_ax.set_title('Загальна тривалість дорам,  які вийшли у 2022 році')

mng = plt.get_current_fig_manager()

plt.show()
