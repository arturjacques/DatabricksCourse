# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Funções

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##SETUP

# COMMAND ----------

from functions.data_creation import create_compras_de_usuario

# COMMAND ----------

df = create_compras_de_usuario(spark)
