# Databricks notebook source
from random import randint, choice
from pyspark.sql import Row
from pyspark.sql import DataFrame

# COMMAND ----------

storage_account_name = "dlstraining01"
container_name = "raw"
sas_token = dbutils.secrets.get('kv-training-03', 'dlstraining01ownertoken')

spark.conf.set(
    f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net",
    sas_token
)

data_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/"

# COMMAND ----------

class CreateComprasData:
    def __init__(self):
        pass

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

        return spark.createDataFrame(row_list)
    
    def _save_csv_data_and_move(self, df: DataFrame):
        path_csv = data_path+"/tmp/compras_saved"
        dbutils.fs.rm(path_csv, True)
        path_sink = data_path+"/compras/"

        df.coalesce(1).write.format('csv').save(path_csv, header=True)

        csv_file = [row.path for row in dbutils.fs.ls(path_csv) if (row.path[-4:]=='.csv')][0]

        dbutils.fs.mv(csv_file, path_sink)
        dbutils.fs.rm(path_csv, True)

    def create_and_save_data(self, number_of_rows: int=100):
        df = self._create_data(number_of_rows)
        self._save_csv_data_and_move(df)

# COMMAND ----------

for i in range(0, 3):
    CreateComprasData().create_and_save_data(1000)
