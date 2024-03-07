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

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, BooleanType, FloatType, DecimalType

from pyspark.sql.functions import from_json, col, explode, posexplode

schema = ArrayType(
            StructType([
                StructField("id_produto", StringType()),
                StructField("valor", DecimalType(18, 2)),
                StructField("impostos", DecimalType(18,2))
            ])
    )

df_json_treated = df_json.withColumn("json_field", from_json(col('value'), schema)).drop('value')

df_json_treated.display()

# COMMAND ----------

df_json_exploded = df_json_treated.select("*", explode("json_field").alias("compra_json")).drop('json_field')

df_json_exploded.display()

# COMMAND ----------

df_json_exploded_melted = df_json_exploded.select("compra_json.*")

df_json_exploded_melted.display()
