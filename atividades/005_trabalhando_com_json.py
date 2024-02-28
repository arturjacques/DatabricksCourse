# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Exercício 1
# MAGIC
# MAGIC <p>
# MAGIC   No df_json crie o Schema da coluna value e salva na tabela, depois exploda os valores do array em linhas e então transforme cada chave do documento em uma coluna.
# MAGIC </p>

# COMMAND ----------

from pyspark.sql import Row
import json

lista = [
    {"id_produto": 1, "valor": 10.0, "impostos": 1.5},
    {"id_produto": 2, "valor": 15.0, "impostos": 2.5},
    {"id_produto": 3, "valor": 20.0, "impostos": 3.0},
    {"id_produto": 1, "valor": 12.0, "impostos": 1.8},
    {"id_produto": 2, "valor": 18.0, "impostos": 3.0}
]

df_json = spark.createDataFrame([Row(value = json.dumps(lista))])

df_json.display()
