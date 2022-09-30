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


def check_anomaly(df, metric, a=3, n=5):
    # алгоритм поиска аномалий с помощью межквартильного размаха
    # shift сдвигает индекс на один период 
    # rolling - скользящее окно
    df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
    df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    # значение межквартильного размаха
    df['iqr'] = df['q75'] - df['q25']
    # значения границ
    df['up'] = df['q75'] + a*df['iqr']
    df['low'] = df['q25'] - a*df['iqr']
    
    # center устанавливает метки в середине окна
    df['up'].rolling(n, center=True, min_periods=1).mean()
    df['low'].rolling(n, center=False, min_periods=1).mean()
    
    if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
        is_alert = 1
    else:
        is_alert = 0
        
    return is_alert, df
    
def run_alerts(chat = None):
    
    # система алертов
    chat_id = chat or 1308226189
    bot = telegram.Bot(token=my_token)
    
    # достаём данные по ленте
    q1 = """ SELECT 
                toStartOfFifteenMinutes(time) as ts,
                toDate(time) as date,
                formatDateTime(ts, '%R') as hm,
                uniqExact(user_id) as users_feed,
                countIf(user_id, action = 'view') as views,
                countIf(user_id, action = 'like') as likes,
                countIf(user_id, action = 'like')/countIf(user_id, action = 'view') as CTR
            FROM simulator_20220820.feed_actions
            WHERE time >= yesterday() and time< toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts"""
    data_1 = pandahouse.read_clickhouse(q1, connection=connection)
    
    # достаём данные по мессенджеру
    q2 = """SELECT 
                    toStartOfFifteenMinutes(time) as ts,
                    toDate(time) as date,
                    formatDateTime(ts, '%R') as hm,
                    uniqExact(user_id) as users_message,
                    COUNT(user_id) as sent_message
            FROM simulator_20220820.message_actions
            WHERE time >= today()- 1  AND time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, hm
            ORDER BY ts"""
    data_2 = pandahouse.read_clickhouse(q2, connection=connection)
    
    # объединяем датафреймы
    data = data_1.merge(data_2, on=['ts','date','hm'], how='outer')
    
    metrics_list = ['users_feed', 'users_message','views', 'likes', 'CTR'] 
    for metric in metrics_list:
        print(metric)
        df = data[['ts', 'date', 'hm', metric]].copy()
        is_alert, df = check_anomaly(df, metric)
        
        if is_alert == 1 or True:
            msg = f"ALERT❗️Метрика {metric}:\n   - текущее значение {df[metric].iloc[-1]:.2f};\n   - отклонение от предыдущего значения {abs(1-df[metric].iloc[-1]/df[metric].iloc[-2]):.2f}%\nОперативный дашборд см. по ссылке: https://superset.lab.karpov.courses/superset/dashboard/1785/"
            sns.set(sns.set(rc={'figure.figsize':(16, 12)}))
            plt.tight_layout()
            
            x = df['ts']
            ax = sns.lineplot(x=x, y=df[metric], label = 'metric')
            ax = sns.lineplot(x=x, y=df['up'], label = 'up')
            ax = sns.lineplot(x=x, y=df['low'], label = 'low')

            # каждый 15 tick по х
            ax.set_xticks(x)
            for ind, label in enumerate(ax.set_xticklabels(labels=x.dt.strftime('%d/%m %H:%M'), rotation=45)):
                if ind % 15 == 0:
                    label.set_visible(True)
                else: 
                    label.set_visible(False)   
                    
            ax.tick_params(axis='both', labelsize=14)       
            ax.set_xlabel('time', fontsize = 16)
            ax.set_ylabel(metric, fontsize = 16)
            
            ax.set_title(metric, fontsize=16)
            # границы оси Y от 0 до неизвестного
            ax.set(ylim = (0, None))
            
            plot_object = io.BytesIO() # создаём файловый объект
            plt.savefig(plot_object, dpi=200)
            plot_object.seek(0) # переводим курсор
            plot_object.name = 'alert_metric.png'
            plt.close() # закрываем графики
            
            bot.sendMessage(chat_id=chat_id, text=msg)
            bot.sendPhoto(chat_id=chat_id, photo=plot_object)
    return 


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def loginova_hw_alerts():

    @task
    def alert_metrics():
        run_alerts(chat = None)
    
    alert_metrics()
    
loginova_hw_alerts = loginova_hw_alerts()