from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph

# декоратор airflow, который помогает создать даг и таск
from airflow.decorators import dag, task

# функция для операторов питона
from airflow.operators.python import get_current_context

def ch_get_df(query = 'Select 1', host= 'https://clickhouse.lab.karpov.courses', user= 'student', 
              password= 'dpo_python_2020'):
    r = requests.post(host, data=query.encode('utf-8'),auth= (user, password), verify=False)
    result = pd.read_csv(StringIO(r.text), sep = '\t')
    return result

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'm-loginova-10',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 10),
}

# Интервал запуска DAG
schedule_interval = '0 10 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_loginova_homework():
    @task() 
    # достаём данные по пользователям ленты
    def extract_from_feed():
        query_1 = """SELECT user_id,
                    toDate(time) AS event_date, 
                    os, 
                    gender,
                    age,
                    COUNT(action = 'view') as views,
                    COUNT(action = 'like') as likes
                    FROM simulator_20220820.feed_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY user_id, event_date, os, gender, age
                    format TSVWithNames"""
        df_feed = ch_get_df(query=query_1)
        return df_feed
    @task() 
    # достаём данные пользователей мессенджера
    def extract_from_message():
        query_2 = """SELECT user_id, event_date, messages_sent, messages_recieved, 
                    users_sent, users_recieved, 
                    os, gender, age 
                    FROM
                    /* число отправленных сообщений по отправителям */
                    (SELECT user_id, 
                        COUNT(reciever_id) AS messages_sent, 
                        COUNT(DISTINCT reciever_id) AS users_sent, 
                        toDate(time) AS event_date,
                        os, gender, age
                    FROM simulator_20220820.message_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY user_id, event_date, os, gender, age) query_1
                    LEFT JOIN
                    /* число полученных сообщений по получателям */
                    (SELECT reciever_id, 
                        COUNT(DISTINCT(user_id)) AS users_recieved, 
                        COUNT(user_id) AS messages_recieved,
                        toDate(time) AS event_date,
                        os, gender, age
                    FROM simulator_20220820.message_actions
                    WHERE toDate(time) = yesterday()
                    GROUP BY reciever_id, event_date, os, gender, age) query_2
                    ON query_1.user_id = query_2.reciever_id
                    format TSVWithNames"""
        df_message = ch_get_df(query=query_2)
        return df_message
    
    @task()
    # объединяем 2 таблицы в одну
    def transform_into_df(df_feed, df_message):
        df_joined = df_feed.join(df_message.set_index(['user_id','event_date','os', 'gender','age']), \
                              on=['user_id','event_date','os', 'gender','age'], \
                              how = 'inner').set_index(['user_id'])
        return df_joined
    
    @task()
    # срез по os
    def transform_by_os(df_joined):
        df_by_os = df_joined[['event_date','os', 'views','likes','messages_sent', 'messages_recieved', \
                     'users_sent', 'users_recieved']].groupby(['event_date', 'os']).sum()
        # через дроп
        # df_by_os = df_joined.drop(['age', 'gender'], axis=1).groupby(['event_date', 'os']).sum()
        return df_by_os
    
    @task()
    # срез по age
    def transform_by_age(df_joined):
        df_by_age = df_joined.drop(['os', 'gender'], axis=1).groupby(['event_date', 'age']).sum() 
        return df_by_age
    
    @task()
    # срез по gender
    def transform_by_gender(df_joined):
        df_by_gender = df_joined.drop(['os', 'age'], axis=1).groupby(['event_date', 'gender']).sum() 
        return df_by_gender
        
    @task()
    # объединяем срезы в датафрейм
    def df_result(df_by_os, df_by_gender, df_by_age):
        df_res = pd.concat([df_by_os, df_by_gender, df_by_age], keys=['os', 'gender', 'age'], \
                           names=['event_date', 'dimension', 'dimension_value']).reset_index()
        return df_res
    
    @task()
    # выгрузка в таблицу clickhouse
    
    def load_to_ch(df_res):
        connection_res = {'host': 'https://clickhouse.lab.karpov.courses',
                      'database': 'test',
                      'user': 'student-rw',
                      'password': '656e2b0c9c'}
    
        query_res = """CREATE TABLE IF NOT EXISTS test.loginova_hw_dag
                        (event_date Date,
                         dimension String,
                         dimension_value String,
                         views UInt64,
                         likes UInt64,
                         messages_sent UInt64,
                         messages_received UInt64,
                         users_sent UInt64,
                         users_received UInt64)
                         ENGINE = Log() """
    
        ph.execute(connection=connection_res, query=query_res)
        ph.to_clickhouse(df = df_res, table='loginova_hw_dag', index=False, connection = connection_res)
    
    df_feed = extract_from_feed()
    df_message = extract_from_message()
    df_joined = transform_into_df(df_feed, df_message)
    df_by_os = transform_by_os(df_joined)
    df_by_age = transform_by_age(df_joined)
    df_by_gender = transform_by_gender(df_joined)
    df_res = df_result(df_by_os, df_by_gender, df_by_age)
    load_to_ch(df_res)
    
dag_loginova_homework = dag_loginova_homework()