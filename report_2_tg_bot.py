# Соберите единый отчет по работе всего приложения. В отчете должна быть информация и по ленте новостей, и по сервису отправки сообщений. Автоматизируйте отправку отчета с помощью Airflow.

import requests
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

import telegram
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import io
import pandahouse


# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'm-loginova-10',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 13),
}

connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator_20220820'
    }

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

def every_2nd(lst: list) -> list:
    return [item for sublist in [(t, "") for t in lst[::2]] for item in sublist]


def text_message(chat = None):
    chat_id = chat or 1308226189
    my_token = '5695779314:AAFu6_wFjCz9JHqm5RDlc3WTV2oZWtByERw'
    bot = telegram.Bot(token=my_token)

    # сколько юзеров к нам вчера пришли (сообщение)
    q_new_users = """SELECT COUNT(user_id) AS "Новые пользователи" FROM
                                    (SELECT user_id, MIN(toDate(time)) AS start_date
                                    FROM simulator_20220820.feed_actions
                                    WHERE start_date = yesterday()
                                    GROUP BY user_id)"""

    # ретеншн первого дня за вчера(сообщение)
    q_retention_1 = """SELECT COUNT(user_id) AS users, date, start_date
                        FROM 
                            (SELECT user_id, MIN(toDate(time)) AS start_date
                            FROM simulator_20220820.feed_actions
                            WHERE start_date = yesterday()-1
                            GROUP BY user_id) q1
                            JOIN
                            (SELECT DISTINCT user_id,
                                            toDate(time) AS date
                            FROM simulator_20220820.feed_actions
                            WHERE toDate(time) != today()) q2 using(user_id)
                        GROUP BY date, start_date
                        ORDER BY date """



    # отправляем сообщение
    df_new_users = pandahouse.read_clickhouse(q_new_users, connection=connection)
    df_q_retention_1 = pandahouse.read_clickhouse(q_retention_1, connection=connection)
    msg = f"Доброе утро,чат! Основные показатели приложения представлены в отчёте. \nВчера к нам пришло {df_new_users.iat[0,0]} новых юзеров\nRetention первого дня вчера составил {df_q_retention_1.iat[1,0]/df_q_retention_1.iat[0,0]*100:.2f}%\nОсновную информацию об аудитории см. в прикреплённом файле.\nХорошего рабочего дня!"
    bot.sendMessage(chat_id=chat_id, text=msg)
    
    
def picture_message(chat=None):
    chat_id = chat or 1308226189
    my_token = '5695779314:AAFu6_wFjCz9JHqm5RDlc3WTV2oZWtByERw'
    bot = telegram.Bot(token=my_token)

    # DAU ленты новостей по каналам
    q_dau_f =    """SELECT toDate(time) AS "Дата", source, count(DISTINCT user_id) AS "DAU"
                    FROM simulator_20220820.feed_actions
                    WHERE toDate(time) >= today()-14
                    GROUP BY toDate(time), source
                    ORDER BY "Дата" ASC"""

    # Среднее и медианное количество просмотров ленты в день на одного юзера
    q_count_view =    """SELECT "Дата", AVG("Просмотры") AS "Среднее", median("Просмотры")  AS "Медиана"
                        FROM (
                            SELECT toDate(time) AS "Дата", COUNT(action) as "Просмотры", user_id
                            FROM simulator_20220820.feed_actions
                            WHERE action = 'view'
                            GROUP BY user_id, toDate(time)
                            )
                        WHERE "Дата" <= yesterday() and "Дата" > yesterday()-14
                        GROUP BY "Дата" """

    # ретеншн для когорты, которая пришла 28 дней назад
    q_retention_28 = """SELECT * FROM
                     (SELECT MAX(users) AS max_value
                      FROM
                        (SELECT COUNT(user_id) AS users, date, start_date
                         FROM 
                            (SELECT user_id, MIN(toDate(time)) AS start_date
                            FROM simulator_20220820.feed_actions
                            WHERE start_date = today() - 28
                            GROUP BY user_id) q1
                         JOIN
                           (SELECT DISTINCT user_id,
                                            toDate(time) AS date
                            FROM simulator_20220820.feed_actions) q2 using(user_id)
                         GROUP BY date, start_date) q2
                      ) query_1
                    CROSS JOIN
                     (SELECT COUNT(user_id) AS users, date, start_date
                      FROM
                        (SELECT user_id,
                                MIN(toDate(time)) AS start_date
                         FROM simulator_20220820.feed_actions
                         WHERE start_date = today() - 28
                         GROUP BY user_id) q1
                      JOIN
                        (SELECT DISTINCT user_id, toDate(time) AS date
                         FROM simulator_20220820.feed_actions) q2 using(user_id)
                      GROUP BY date, start_date) query_2
                      ORDER BY date ASC"""

    # максимальное количество пользователей по часам за вчера в ленте
    q_pccu_f = """SELECT toStartOfHour(toDateTime(time)) AS time, 
                       max("Макс. кол-во пользователей") AS "Макс. кол-во пользователей"
                    FROM
                      (SELECT toStartOfHour(toDateTime(timestamp)) AS time,
                              max("max(users)") AS "Макс. кол-во пользователей"
                       FROM
                         (SELECT time as timestamp,
                                         MAX(users)
                          FROM
                            (SELECT toDateTime(time) as time,
                                    COUNT(user_id) as users
                             FROM simulator_20220820.feed_actions
                             GROUP BY time) users_amount
                          GROUP BY timestamp
                          ORDER BY 2 DESC) AS virtual_table
                       GROUP BY toStartOfHour(toDateTime(timestamp))
                       ORDER BY "Макс. кол-во пользователей" DESC) AS virtual_table
                    WHERE time >= toDateTime('2022-09-15 00:00:00')
                      AND time < toDateTime('2022-09-16 00:00:00')
                    GROUP BY time
                    ORDER BY time
                    LIMIT 1000"""

    # максимальное количество пользователей по часам за вчера в ленте
    q_pccu_m = """SELECT toStartOfHour(toDateTime(time)) AS time,
                       max("Макс. кол-во пользователей") AS "Макс. кол-во пользователей"
                FROM
                  (SELECT toStartOfHour(toDateTime(timestamp)) AS time,
                          max("max(users)") AS "Макс. кол-во пользователей"
                   FROM
                     (SELECT time as timestamp,
                                     MAX(users)
                      FROM
                        (SELECT toDateTime(time) as time,
                                COUNT(user_id) as users
                         FROM simulator_20220820.message_actions
                         GROUP BY time) users_amount
                      GROUP BY timestamp
                      ORDER BY 2 DESC) AS virtual_table
                   GROUP BY toStartOfHour(toDateTime(timestamp))
                   ORDER BY "Макс. кол-во пользователей" DESC) AS virtual_table
                WHERE time >= toDateTime('2022-09-15 00:00:00')
                  AND time < toDateTime('2022-09-16 00:00:00')
                GROUP BY time
                ORDER BY time
                LIMIT 1000;"""
    # строим графики 
    sns.set(rc={'figure.figsize':(16, 12)})
    fig, axes = plt.subplots(2, 2)
    plt.subplots_adjust(wspace = 0.15, hspace=0.5)
    # DAU по каналам
    df_dau_f = pandahouse.read_clickhouse(q_dau_f, connection=connection)
    x_dau_f = df_dau_f[df_dau_f["source"] == "ads"]["Дата"].dt.strftime('%d/%m/%Y')
    y1 = df_dau_f[df_dau_f['source'] == 'organic']['DAU']
    y2 = df_dau_f[df_dau_f['source'] == 'ads']['DAU']
    df_dau_f_for_plot = pd.DataFrame({"organic": y1.values, "ads": y2.values}, index=x_dau_f)
    pic1 = sns.lineplot(data=df_dau_f_for_plot, ax=axes[0,0])
    pic1.set_xticklabels(labels=x_dau_f, rotation=30)

    # Количество просмотров ленты на одного юзера по дням (среднее и медианное значения)
    df_count_view = pandahouse.read_clickhouse(q_count_view, connection=connection)
    x = df_count_view["Дата"].dt.strftime('%d/%m/%Y')
    y1 = df_count_view['Среднее']
    y2 = df_count_view['Медиана']
    df_count_view_for_plot = pd.DataFrame({"Среднее": y1.values, "Медиана": y2.values}, index=x)
    pic2 = sns.lineplot(data=df_count_view_for_plot, ax=axes[0,1])
    pic2.set_xticklabels(labels=x, rotation=30)
    
    # pccu feed
    df_pccu_f = pandahouse.read_clickhouse(q_pccu_f, connection=connection)
    x = df_pccu_f['time'].dt.strftime('%d/%m %H:%M')
    y = df_pccu_f['Макс. кол-во пользователей']
    pic3 = sns.lineplot(x,y, ax=axes[1,0])
    pic3.set_xticklabels(labels=every_2nd(x), rotation=45)

    # pccu messages
    df_pccu_m = pandahouse.read_clickhouse(q_pccu_m, connection=connection)
    x = df_pccu_m['time'].dt.strftime('%d/%m %H:%M')
    y = df_pccu_m['Макс. кол-во пользователей']
    pic4 = sns.lineplot(x,y, ax=axes[1,1])
    pic4.set_xticklabels(labels=every_2nd(x), rotation=45)

    pic1.title.set_text('DAU по каналам')
    pic2.title.set_text('Кол-во просмотров ленты одним юзером')
    pic3.title.set_text('PCCU ленты')
    pic4.title.set_text('PCCU мессенджера')

    plot_object = io.BytesIO() # создаём файловый объект
    plt.savefig(plot_object, dpi=200)
    plot_object.seek(0) # переводим курсор
    plot_object.name = 'plots.png'
    plt.close() # закрываем графики
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    # retention для когорты, пришедшей 28 дней назад
    sns.set(rc={'figure.figsize':(18, 6)})
    df_r = pandahouse.read_clickhouse(q_retention_28, connection=connection)
    x = df_r['date'].dt.strftime('%d/%m/%Y')
    y = df_r['users']
    pic5 = sns.lineplot(x,y)
    pic5.set_xticklabels(labels=x, rotation=30)
    pic5.title.set_text('Retention')


    plot_object = io.BytesIO() # создаём файловый объект
    plt.savefig(plot_object, dpi=200)
    plot_object.seek(0) # переводим курсор
    plot_object.name = 'plots.png'
    plt.close() # закрываем графики
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

def file_message(chat = None):
    # файл с данными по аудитории приложения
    
    # файл активная аудитория
    q_active_users = """SELECT age AS age,
                               os AS os,
                               gender AS gender,
                               count(DISTINCT user_id) AS "Уникальные пользователи",
                               COUNT(action)/COUNT(DISTINCT user_id) AS "Кол-во действий на пользователя"
                        FROM simulator_20220820.feed_actions
                        GROUP BY age,
                                 os,
                                 gender
                        ORDER BY "Уникальные пользователи" DESC
                        LIMIT 10000;"""

    df_q_active_users = pandahouse.read_clickhouse(q_active_users, connection=connection)
    file_object = io.StringIO()
    df_q_active_users.to_csv(file_object)
    file_object.seek(0)
    file_object.name = 'app_users.csv'
    bot.send_document(chat_id=chat_id, document=file_object)


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def loginova_report_2():

    @task
    def message_with_metrics():
        text_message(chat = None)
    
    message_with_metrics()
    
    @task
    def pictures_with_metrics():
        picture_message(chat=None)
        
    pictures_with_metrics()
    
    @task
    def file_app_users():
        file_message(chat=None)
        
    file_app_users()
    
loginova_report_2 = loginova_report_2()