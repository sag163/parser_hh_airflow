from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils.operator import GetVacancyOperator
from datetime import timedelta, datetime
from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
import telegram
from airflow.models import Variable


with DAG(
        dag_id='search_vacancy3',
        start_date=datetime(2022, 9, 9),
        schedule_interval=timedelta(hours=12),
) as dag:

    create_table = PostgresOperator(
        task_id='create_table_task',
        sql='sql_query/create_table.sql',
        postgres_conn_id='postgres_default',
    )

    get_vacansies = GetVacancyOperator(
        task_id='get_vacansies',
        conn_id='hh_conn_id',
        dag=dag,
        do_xcom_push=True,
    )

    @task(task_id=f'insert_data')
    def insert_data_task(**kwargs):
        """Функция добавления записей в БД"""
        try:

            params = kwargs['ti'].xcom_pull(
                task_ids='get_vacansies', key="return_value")
            print('params', params)
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            engine = pg_hook.get_sqlalchemy_engine()
            for param in params:
                try:
                    engine.execute(param[0])

                except Exception as error:

                    print(
                        'error', f"Error while execute sql query, error:{error}")

        except Exception as error:
            print(
                'error', f"Error while insert data, error:{error}")

    @task(task_id='send_vacancy_to_telegram')
    def send_vacancy_to_telegram(**kwargs):
        """Функция добавления записей в БД"""
        try:

            params = kwargs['ti'].xcom_pull(
                task_ids='get_vacansies', key="return_value")
            pg_hook = PostgresHook(postgres_conn_id='postgres_default')
            engine = pg_hook.get_sqlalchemy_engine()

            TELEGRAM_TOKEN = Variable.get('TELEGRAM_TOKEN')
            CHAT_ID = Variable.get('CHAT_ID')
            for param in params:
                try:
                    cursor = engine.execute(param[3])
                    answer = cursor.fetchall()
                    if answer[0][0] is False:
                        bot = telegram.Bot(token=TELEGRAM_TOKEN)
                        bot.sendMessage(chat_id=CHAT_ID, text=param[2])

                except Exception as error:
                    print(f"Error while execute sql query, error:{error}")

        except Exception as error:
            print(f"Error while insert data, error:{error}")

    insert = insert_data_task()
    send_vacancy_to_telegram = send_vacancy_to_telegram()

    create_table >> get_vacansies >> insert >> send_vacancy_to_telegram
