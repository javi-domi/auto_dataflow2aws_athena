import boto3
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from airflow.decorators import task, dag


AWS_S3_BUCKET = '1sti-data-engineer-challenge'
AWS_ACCESS_KEY_ID = 'aws_access_key_id'
AWS_SECRET_ACCESS_KEY = 'aws_secret_access_key'


@dag(schedule_interval='@daily', start_date=datetime(2022, 8, 8), catchup=False)
def pipeline_dag():

    @task
    def get_data_escolas_publicas():
        url_ouro = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosOuro.do.htm'
        url_prata = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosPrata.do.htm'
        url_bronze = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosBronze.do.htm'
        # url_honrosas = ''
        data_ouro = requests.get(url_ouro).text
        data_prata = requests.get(url_prata).text
        data_bronze = requests.get(url_bronze).text
        soup_ouro = BeautifulSoup(data_ouro, 'html')
        soup_prata = BeautifulSoup(data_prata, 'html')
        soup_bronze = BeautifulSoup(data_bronze, 'html')

        df = pd.DataFrame(
            columns=['Nome', 'Escola', 'Tipo', 'Municipio', 'UF', 'Medalha'])
        for table in soup_ouro.find_all('table'):
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
        for table in soup_prata.find_all('table'):
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
        for table in soup_bronze.find_all('table'):
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
        print(df.describe())
        df.to_csv('data_escolas_publicas.csv', index=False)

    @task
    def get_data_escolas_privadas():
        url_ouro = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosOuro.privada.do.htm'
        url_prata = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosPrata.privada.do.htm'
        url_bronze = 'http://premiacao.obmep.org.br/16aobmep/verRelatorioPremiadosBronze.privada.do.htm'
        # url_honrosas = ''
        data_ouro = requests.get(url_ouro).text
        data_prata = requests.get(url_prata).text
        data_bronze = requests.get(url_bronze).text
        soup_ouro = BeautifulSoup(data_ouro, 'html')
        soup_prata = BeautifulSoup(data_prata, 'html')
        soup_bronze = BeautifulSoup(data_bronze, 'html')

        df = pd.DataFrame(
            columns=['Nome', 'Escola', 'Tipo', 'Municipio', 'UF', 'Medalha'])
        for table in soup_ouro.find_all('table'):
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
        for table in soup_prata.find_all('table'):
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
        for table in soup_bronze.find_all('table'):
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
        print(df.describe())
        df.to_csv('data_escolas_privadas.csv', index=False)

    @task
    def join_dataframes_in_datalake():
        df_publicas = pd.read_csv('data_escolas_publicas.csv')
        df_privadas = pd.read_csv('data_escolas_privadas.csv')
        df = pd.concat([df_publicas, df_privadas])
        key = 'data_medalhas.snappy.parquet'
        df.to_parquet(
            f"s3://{AWS_S3_BUCKET}/{key}",
            index=False,
            compression='snappy',
            storage_options={
                "key": AWS_ACCESS_KEY_ID,
                "secret": AWS_SECRET_ACCESS_KEY,
            },
        )
        print(df.describe())
        print('Joining dataframes and upload to datalake')

    [get_data_escolas_publicas(), get_data_escolas_privadas()] >> \
        join_dataframes_in_datalake()


dag = pipeline_dag()
