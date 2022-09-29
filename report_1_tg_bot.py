# Создайте своего телеграм-бота. Напишите скрипт для сборки отчета по ленте новостей. Отчет должен состоять из двух частей:

# 1. текст с информацией о значениях ключевых метрик за предыдущий день;
# 2. график с значениями метрик за предыдущие 7 дней.
# Отобразите в отчете следующие ключевые метрики: DAU, Просмотры, Лайки, CTR.
# Автоматизируйте отправку отчета с помощью Airflow. 

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
    'start_date': datetime(2022, 9, 10),
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

def text_message(chat = None):
    chat_id = chat or 1308226189
    my_token = '5695779314:AAFu6_wFjCz9JHqm5RDlc3WTV2oZWtByERw'
    bot = telegram.Bot(token=my_token)
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator_20220820'
    }

    q_dau=   """SELECT toDate(time) AS "Дата",
                count(DISTINCT user_id) AS "DAU"
                FROM simulator_20220820.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY toDate(time);"""

    q_views= """SELECT toDate(time) AS "Дата",
                count(action) AS "Просмотры"
                FROM simulator_20220820.feed_actions
                WHERE toDate(time) = yesterday() AND action = 'view'
                GROUP BY toDate(time);"""

    q_likes ="""SELECT toDate(time) AS "Дата",
                count(action) AS "Лайки"
                FROM simulator_20220820.feed_actions
                WHERE toDate(time) = yesterday() AND action = 'like'
                GROUP BY toDate(time);"""

    q_ctr =  """SELECT toDate(time) AS "Дата",
                countIf (user_id, action = 'like') / countIf (user_id, action = 'view') AS "CTR"
                FROM simulator_20220820.feed_actions
                WHERE toDate(time) = yesterday()
                GROUP BY toDate(time);"""

    df_dau = pandahouse.read_clickhouse(q_dau, connection=connection)
    df_views = pandahouse.read_clickhouse(q_views, connection=connection)
    df_likes = pandahouse.read_clickhouse(q_likes, connection=connection)
    df_ctr = pandahouse.read_clickhouse(q_ctr, connection=connection)

    daily_report_table = df_dau.set_index(['Дата']).join([df_views.set_index(['Дата']), \
                                                  df_likes.set_index(['Дата']), \
                                                  df_ctr.set_index(['Дата'])]).reset_index()
    msg = f"Ключевые метрики за вчерашний день {daily_report_table.iat[0,0].date()} \nDAU {daily_report_table.iat[0,1]}\nПросмотры {daily_report_table.iat[0,2]}\nЛайки {daily_report_table.iat[0,3]}\nCTR {daily_report_table.iat[0,4]:.2}"
    bot.sendMessage(chat_id=chat_id, text=msg)
    
    
def picture_message(chat=None):
    chat_id = chat or 1308226189
    my_token = '5695779314:AAFu6_wFjCz9JHqm5RDlc3WTV2oZWtByERw'
    bot = telegram.Bot(token=my_token)
    connection = {
        'host': 'https://clickhouse.lab.karpov.courses',
        'password': 'dpo_python_2020',
        'user': 'student',
        'database': 'simulator_20220820'
    }
    sns.set(rc={'figure.figsize':(14, 12)})
    fig, axes = plt.subplots(2, 2)
    plt.subplots_adjust(wspace = 0.25, hspace=0.5)
    q_dau_w=   """SELECT toDate(time) AS "Дата",
                count(DISTINCT user_id) AS "DAU"
                FROM simulator_20220820.feed_actions
                WHERE toDate(time) <= yesterday() and toDate(time) > yesterday()-7
                GROUP BY toDate(time)"""
    df_dau_w = pandahouse.read_clickhouse(q_dau_w, connection=connection)
    x = df_dau_w['Дата'].dt.strftime('%m/%d/%Y')
    y = df_dau_w['DAU']
    pic1 = sns.lineplot(x,y, ax=axes[0,0])
    pic1.set_xticklabels(labels=x, rotation=30)


    q_views_w =  """SELECT toDate(time) AS "Дата",
                    count(action) AS "Просмотры"
                    FROM simulator_20220820.feed_actions
                    WHERE action = 'view' AND toDate(time) <= yesterday() AND toDate(time) > yesterday()-7
                    GROUP BY toDate(time)"""
    df_views_w = pandahouse.read_clickhouse(q_views_w, connection=connection)
    x = df_views_w['Дата'].dt.strftime('%m/%d/%Y')
    y = df_views_w['Просмотры']
    pic2 = sns.lineplot(x,y,ax=axes[0,1])
    pic2.set_xticklabels(labels=x, rotation=30)

    q_likes_w =  """SELECT toDate(time) AS "Дата",
                    count(action) AS "Лайки"
                    FROM simulator_20220820.feed_actions
                    WHERE action = 'like' AND toDate(time) <= yesterday() AND toDate(time) > yesterday()-7
                    GROUP BY toDate(time)"""
    df_likes_w = pandahouse.read_clickhouse(q_likes_w, connection=connection)
    x = df_likes_w['Дата'].dt.strftime('%m/%d/%Y')
    y = df_likes_w['Лайки']
    pic3 = sns.lineplot(x,y, ax=axes[1,0])
    pic3.set_xticklabels(labels=x, rotation=30)

    q_ctr_w =  """SELECT toDate(time) AS "Дата",
                countIf (user_id, action = 'like') / countIf (user_id, action = 'view') AS "CTR"
                FROM simulator_20220820.feed_actions
                WHERE toDate(time) <= yesterday() and toDate(time) > yesterday()-7
                GROUP BY toDate(time)"""
    df_ctr_w = pandahouse.read_clickhouse(q_ctr_w, connection=connection)
    x = df_ctr_w['Дата'].dt.strftime('%m/%d/%Y')
    y = df_ctr_w['CTR']
    pic4 = sns.lineplot(x,y, ax=axes[1,1])
    pic4.set_xticklabels(labels=x, rotation=30)
    pic1.title.set_text('DAU')
    pic2.title.set_text('Просмотры')
    pic3.title.set_text('Лайки')
    pic4.title.set_text('CTR')

    plot_object = io.BytesIO() # создаём файловый объект
    plt.savefig(plot_object, dpi=200)
    plot_object.seek(0) # переводим курсор
    plot_object.name = 'week_metrics.png'
    plt.close() # закрываем графики
    bot.sendPhoto(chat_id=chat_id, photo=plot_object)

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def loginova_report_1():

    @task()
    def daily_metrics():
        text_message(chat = -555114317)
    
    daily_metrics()
    
    @task()
    def weekly_metrics():
        picture_message(chat = -555114317)
        
    weekly_metrics()
    
loginova_report_1 = loginova_report_1()
