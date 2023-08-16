import hashlib
import json
import re
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from elasticsearch import helpers
from loguru import logger
from requests import Response
import locale
from timeit import default_timer as timer
from dateutil.relativedelta import relativedelta

from src.config.elastic import elastic_client, ElasticConfig


class Cnbc:
    def __init__(self):
        self._es_client = elastic_client()

    def scraping_news(self):
        links = []
        datas = []
        # date = datetime.now().strftime("%Y/%m/%d")
        for index in range(0, 365):
            date = datetime.now() - relativedelta(days=index)
            url = f'https://www.cnbcindonesia.com/market/indeks/5?date={date.strftime("%Y/%m/%d")}'
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            text_centers = soup.find(class_='text_center')
            if text_centers:
                text_centers = text_centers.findAll("a")
                for text_center in text_centers:
                    number = text_center.text
                    if number.isnumeric():
                        new_url = f'https://www.cnbcindonesia.com/market/indeks/5/{number}?date={date}'
                        response_page = requests.get(new_url)
                        page_soup = BeautifulSoup(response_page.content, "html.parser")
                        media_rows = page_soup.find(class_="media_rows").findAll('a')
                        for media_row in media_rows:
                            href = media_row.get('href')
                            links.append(href)
            else:
                media_rows = soup.find(class_="media_rows").findAll('a')
                for media_row in media_rows:
                    href = media_row.get('href')
                    links.append(href)
            logger.info(f'[!!] starting to process {len(links)} links')
            now = timer()
            for link in links:
                try:
                    html_response = requests.get(link)
                    data = self.parse_content(html_response)
                    data['link'] = link
                    action = {
                        '_index': f'{ElasticConfig().ELASTICSEARCH_INDEX}-{datetime.now().year}',
                        '_id': self.generate_id(data),
                        '_source': data
                    }
                    if not self.get_duplicate(action['_id']):
                        datas.append(action)
                    if len(datas) == 100:
                        helpers.bulk(self._es_client, datas)
                        logger.info(f'[>>] inserting {len(datas)} datas to elastic')
                        logger.info(f'[!!] execution time = {timer() - now}')
                        datas.clear()
                except AttributeError:
                    logger.info(f'[!!] data video found: skipping')
                    logger.info(f'[!!] execution time = {timer() - now}')
        if datas:
            helpers.bulk(self._es_client, datas)
            logger.info(f'[>>] inserting {len(datas)} datas to elastic')
            logger.info(f'[!!] execution time = {timer() - now}')
            datas.clear()

    @staticmethod
    def parse_content(html: Response) -> dict:
        content_soup = BeautifulSoup(html.content, 'html.parser')
        container = content_soup.find_all("h1", limit=1)
        title = container[0].text
        detail_box = content_soup.find_all('div', class_='detail_box')
        date = detail_box[0].find_next(class_='date').text
        date_object = datetime.strptime(date, '%d %B %Y %H:%M')
        formatted_date = date_object.strftime('%Y-%m-%d %H:%M:%S')
        media_article = content_soup.find(class_="media_artikel").findAll('img')
        image = media_article[0].get('src')
        detail_text = content_soup.find(class_='detail_text').findAll('p')
        contents = []
        for text in detail_text:
            content = text.text
            if not re.search(r'ADVERTISEMENT', content) and not re.search(r'SCROLL TO RESUME CONTENT', content):
                contents.append(content)
        return {
            'title': title,
            'created_at': formatted_date,
            'image_url': image,
            'content': ' '.join(contents),
            'source': 'cnbc indonesia'
        }

    @staticmethod
    def generate_id(data: dict) -> str:
        body = {
            'title': data['title'],
            'created_at': data['created_at'],
            'link': data['link']
        }
        return hashlib.md5(json.dumps(body).encode()).hexdigest()

    def get_duplicate(self, id) -> bool:
        body = {
            'query': {
                'match': {
                    '_id': id
                }
            }
        }
        resps = self._es_client.search(index=f'{ElasticConfig().ELASTICSEARCH_INDEX}-{datetime.now().year}',
                                       body=body,
                                       ignore_unavailable=True)
        if resps['hits']['total']['value']:
            return True
        return False


class Cnn:
    def scraping_news(self):
        links = []
        date = datetime.now().strftime("%Y/%m/%d")
        url = f'https://www.cnnindonesia.com/keuangan/indeks/38?date={date}'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        text_centers = soup.find(
            class_='text-white bg-cnn_red inline-flex items-center justify-center w-[30px] h-[30px] rounded-md')
        for text_center in text_centers:
            number = text_center.text
            if number.isnumeric():
                new_url = f'https://www.cnnindonesia.com/keuangan/indeks/38/{number}?date={date}'
                response_page = requests.get(new_url)
                page_soup = BeautifulSoup(response_page.content, "html.parser")
                media_rows = page_soup.find(class_="flex flex-col gap-5").findAll('a')
                for media_row in media_rows:
                    href = media_row.get('href')
                    links.append(href)
        for link in links:
            html_response = requests.get(link)
            data = self.parse_content(html_response)
            data['link'] = link

    @staticmethod
    def parse_content(html: Response) -> dict:
        content_soup = BeautifulSoup(html.content, 'html.parser')
        container = content_soup.find_all("h1", limit=1)
        title = container[0].text
        media_article = content_soup.find(class_="detail-image my-5").findAll('img')
        image = media_article[0].get('src')
        detail_text = content_soup.find(class_='detail-text text-cnn_black text-sm grow min-w-0').findAll('p')
        contents = []
        for text in detail_text:
            content = text.text
            if not re.search(r'ADVERTISEMENT', content) and not re.search(r'SCROLL TO RESUME CONTENT',
                                                                          content) and re.search(r'^[A-Za-z]+$',
                                                                                                 content):
                contents.append(content)
        return {
            'title': title,
            'image': image,
            'content': ' '.join(contents)
        }


class Detik:

    def __init__(self):
        locale.setlocale(locale.LC_TIME, 'id_ID.UTF-8')
        self._es_client = elastic_client()

    def scraping_news(self):
        links = []
        date = datetime.now().strftime("%Y/%m/%d")
        url = f'https://finance.detik.com/finansial/indeks?date={date}'
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")
        pagination = soup.find(class_='pagination text-center mgt-16 mgb-16').findAll('a')
        for page in pagination:
            number = page.text
            if number.isnumeric():
                new_url = f'https://finance.detik.com/finansial/indeks/1?date={date}'
                response_page = requests.get(new_url)
                page_soup = BeautifulSoup(response_page.content, "html.parser")
                media_rows = page_soup.find_all(class_='media__text')
                for media_row in media_rows:
                    links.append(media_row.find_next('a').get('href'))
        for link in links:
            html_response = requests.get(link)
            data = self.parse_content(html_response)
            data['link'] = link
            print(data)

    @staticmethod
    def parse_content(html: Response) -> dict:
        content_soup = BeautifulSoup(html.content, 'html.parser')
        container = content_soup.find(class_="detail__header").findAll('h1')
        title = container[0].text
        formatted_title = re.search(r'^[\s\r\n]*([^.]*)', title).group(1).strip()
        date = content_soup.find(class_='detail__date').text
        date_object = datetime.strptime(date, '%A, %d %b %Y %H:%M WIB')
        formatted_date = date_object.strftime('%Y-%m-%d %H:%M:%S')
        media_article = content_soup.find(class_="detail__media").findAll('img')
        image = media_article[0].get('src')
        detail_text = content_soup.find(class_='detail__body-text itp_bodycontent').findAll('p')
        contents = []
        for text in detail_text:
            content = text.text
            if not re.search(r'ADVERTISEMENT', content) and not re.search(r'SCROLL TO RESUME CONTENT', content):
                contents.append(content)
        return {
            'title': formatted_title,
            'image_url': image,
            'content': ' '.join(contents),
            'created_at': formatted_date,
            'source': 'detik finance'
        }
