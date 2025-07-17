from random import (
                randint,
                choice
)

import uuid
from pyspark.sql import Row
from pyspark.sql import DataFrame
import datetime

produtos = [{"id_product": 1, "nome_do_produto": "camiseta", "preço": 25.00, "peso": 0.2},
            {"id_product": 2, "nome_do_produto": "calça", "preço": 50.00, "peso": 0.4},
            {"id_product": 3, "nome_do_produto": "tênis", "preço": 100.00, "peso": 0.8},
            {"id_product": 4, "nome_do_produto": "mochila", "preço": 80.00, "peso": 1.0},
            {"id_product": 5, "nome_do_produto": "óculos", "preço": 150.00, "peso": 0.05},
            {"id_product": 6, "nome_do_produto": "relógio", "preço": 200.00, "peso": 0.1},
            {"id_product": 7, "nome_do_produto": "fones de ouvido", "preço": 120.00, "peso": 0.15},
            {"id_product": 8, "nome_do_produto": "carregador", "preço": 30.00, "peso": 0.1}]

pessoas = [
    {'id_pessoa': 1, 'nome': 'Ana', 'idade': 25, 'sexo': 'F', 'endereço': 'Rua A, 123, Centro'},
    {'id_pessoa': 2, 'nome': 'Bruno', 'idade': 30, 'sexo': 'M', 'endereço': 'Rua B, 456, Norte'},
    {'id_pessoa': 3, 'nome': 'Carla', 'idade': 28, 'sexo': 'F', 'endereço': 'Rua C, 789, Sul'}
]


def create_compras_de_usuario(spark, n_compras=20, start_date='2022-01-01', end_date='2023-01-01'):
    compras_list = []
    start_date = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end_date = datetime.datetime.strptime(end_date, "%Y-%m-%d")


    for _ in range(n_compras):
        user = choice(pessoas)
        compra_id = str(uuid.uuid4())

        random_seconds = randint(0, int((end_date - start_date).total_seconds()))
        random_date = start_date + datetime.timedelta(seconds=random_seconds)

        for quantidade in range(randint(0, 10)):
            produto = choice(produtos)
            compras_list.append(
                {
                    "compra_id": compra_id,
                    "id_pessoa": user["id_pessoa"],
                    "id_product": produto["id_product"],
                    "compra_ts": random_date,
                    "valor": produto["preço"]
                }
            )

    return spark.createDataFrame(compras_list)

class CreateComprasDataCsv:
    def __init__(self, spark, dbutils):
        self.csv_tmp_path = "/tmp/csv_with_many_files"
        self.sink_path = "/tmp/compras/"
        dbutils.fs.mkdirs(self.sink_path)

        self.spark = spark
        self.dbutils = dbutils

    def _create_data(self, number_of_rows: int=100):
        item_cost = {
            "1": 2.00,
            "2": 2.50,
            "3": 50.00,
            "4": 20.50,
            "5": 1.99
        }

        country_list = ["BR", "US", "MX", "DO", "None"]
        row_list = list()

        for _ in range(0, number_of_rows):
            product_id = str(randint(1, 5))
            product_qt = randint(1, 100)

            row_obj = Row(
                id = randint(0, 10000000),
                user_id=randint(0, 100),
                product_id = product_id,
                product_qt = product_qt,
                value = str(product_qt*item_cost[product_id]),
                country = choice(country_list)
            )

            row_list.append(row_obj)

        return self.spark.createDataFrame(row_list)
    
    def _save_csv_data_and_move(self, df: DataFrame):
        self.dbutils.fs.rm(self.csv_tmp_path, True)


        df.coalesce(1).write.format('csv').save(self.csv_tmp_path, header=True)

        csv_file = [row.path for row in self.dbutils.fs.ls(self.csv_tmp_path) if (row.path[-4:]=='.csv')][0]

        self.dbutils.fs.mv(csv_file, self.sink_path)
        self.dbutils.fs.rm(self.csv_tmp_path, True)

    def create_and_save_data(self, number_of_rows: int=100):
        df = self._create_data(number_of_rows)
        self._save_csv_data_and_move(df)

    def clean_data(self):
        self.dbutils.fs.rm(self.sink_path, True)

def create_compras_de_usuario_list(rows=20):
    compras_list = []

    for _ in range(rows):
        user = choice(pessoas)
        compra_id = str(uuid.uuid4())

        compras_list.append({
            "compra_id": compra_id,
            "id_pessoa": user["id_pessoa"],
            "nome": user["nome"],
            "endereço": user["endereço"],
            "compras": list()
        })

        for _ in range(randint(0, 10)):
            produto = choice(produtos)
            
            compras_list[-1]["compras"].append(
                {
                    "id_product": produto["id_product"],
                    "nome_do_produto": produto["nome_do_produto"],
                    "preço": produto["preço"],
                    "peso": produto["peso"]
                }
            )

    return compras_list