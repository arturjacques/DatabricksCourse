# Databricks notebook source
# MAGIC %md #Conceitos básicos do Databricks

# COMMAND ----------

# MAGIC %md ## Linguagens
# MAGIC
# MAGIC <p>O databricks é uma plataforma baseada em spark para processamento de dados. O processamento de dados pode ser feito em Python, Scala, R e SQL. Existe uma linguagem padrão do notebook configurada e as outras linguagens podem ser utilizadas com o comando mágico "%" e o nome da linaguem.</p>

# COMMAND ----------

print('hello world')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'hello world' AS saudacao
