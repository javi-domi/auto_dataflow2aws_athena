from tracemalloc import start
import boto3
import requests
import sqlalchemy
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from airflow.decorators import task, dag


@dag(schedule_interval='@daily', start_date=datetime(2022, 8, 8), catchup=False)
def pipeline_dag():

    @task
    def get_data_escolas_publicas():
        url_ouro = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosOuro.do.htm'
        url_prata = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosPrata.do.htm'
        url_bronze = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosBronze.do.htm'
        # url_honrosas = ''
        data_ouro = requests.get(url_ouro).text
        soup = BeautifulSoup(data_ouro, 'html')
        table = soup.find_all('table', class_='list')

        print('Classes of each table:')
        for table in soup.find_all('table'):
            print(table.get('class'))

        df = pd.DataFrame(
            columns=['Nome', 'Escola', 'Tipo', 'Municipio', 'UF', 'Medalha'])
        for table in soup.find_all('table'):
            # Collecting Ddata
            for row in table.tbody.find_all('tr'):
                # Find all data for each column
                columns = row.find_all('td')

                if(columns != []):
                    nome = columns[1].text.strip()
                    escola = columns[2].text.strip()
                    tipo = columns[3].text.strip('&0.')
                    municipio = columns[4].text.strip('&0.')
                    uf = columns[5].text.strip('&0.')
                    medalha = columns[6].text.strip('&0.')

                    df = df.append({'Nome': nome,  'Escola': escola,  'Tipo': tipo,
                                   'Municipio': municipio, 'UF': uf, 'Medalha': medalha}, ignore_index=True)
        print(df.head())

    @task
    def get_data_escolas_privadas():
        url_ouro = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosOuro.privada.do.htm'
        url_prata = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosPrata.privada.do.htm'
        url_bronze = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosBronze.privada.do.htm'
        # url_honrosas = ''
        data_ouro = requests.get(url_ouro).text
        soup = BeautifulSoup(data_ouro, 'html')
        table = soup.find_all('table', class_='list')

        print('Classes of each table:')
        for table in soup.find_all('table'):
            print(table.get('class'))

        df = pd.DataFrame(
            columns=['Nome', 'Escola', 'Tipo', 'Municipio', 'UF', 'Medalha'])
        for table in soup.find_all('table'):
            # Collecting Ddata
            for row in table.tbody.find_all('tr'):
                # Find all data for each column
                columns = row.find_all('td')

                if(columns != []):
                    nome = columns[1].text.strip()
                    escola = columns[2].text.strip()
                    tipo = columns[3].text.strip('&0.')
                    municipio = columns[4].text.strip('&0.')
                    uf = columns[5].text.strip('&0.')
                    medalha = columns[6].text.strip('&0.')

                    df = df.append({'Nome': nome,  'Escola': escola,  'Tipo': tipo,
                                   'Municipio': municipio, 'UF': uf, 'Medalha': medalha}, ignore_index=True)
        print(df.head())

    get_data_escolas_publicas()
    get_data_escolas_privadas()


dag = pipeline_dag()
