from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests
import re
from .insert_vacancy import create_vacancy, check_vacancy


class GetVacancyOperator(BaseOperator):

    @apply_defaults
    def __init__(self, conn_id: str, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hh_token = conn_id

    def execute(self, context: Any):
        url = "https://api.hh.ru/vacancies/"

        params = {
            "Authorization": f"Bearer {self.hh_token}",
            "text": "python",
            "area": self.get_area("Самара"),
            "describe_arguments": True,
            "period": 1,
        }
        answer = requests.get(url, params=params)
        vacancies = answer.json()["items"]
        queries = self.transform_query(vacancies)
        return queries

    def get_area(self, city: str) -> int:
        "Получение id города"
        url = "https://api.hh.ru/suggests/areas/"
        params = {
            "Authorization": f"Bearer {self.hh_token}",
            "text": city,
        }
        answer = requests.get(url, params=params)
        return answer.json()["items"][0]["id"]

    def transform_query(self, vacancies: list) -> list:
        "Преобразование данных в запросы"
        queries = []
        for vacancy in vacancies:
            id = vacancy.get('id', None)
            name = vacancy.get("name", None)
            url = vacancy.get("apply_alternate_url", None)
            created_at = vacancy.get("created_at", None)
            published_at = vacancy.get("published_at", None)
            str1 = re.sub('id', id, create_vacancy, 1)
            str2 = re.sub('name_vacancy', name, str1)
            str3 = re.sub('url_vacancy', url, str2)
            str4 = re.sub('created_at', created_at, str3)
            insert_query = re.sub('published_at', published_at, str4)
            message = name + '\n' + url
            check_query = re.sub('id_number', id, check_vacancy)

            queries.append([insert_query, id, message, check_query])
        return queries
