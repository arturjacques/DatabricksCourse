# Databricks notebook source
# MAGIC %md #Conceitos básicos do Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Comandos Básicos
# MAGIC
# MAGIC ## Execução
# MAGIC
# MAGIC <p>O Databricks Utiliza um notebook para executar o código. As células serão executadas em sequência quando for utilizado o botão <b>Run all</b>, mas quando executando iterativamente a execução das celulas fica a critério do programados. Os dados que ficarão em memoria Sobre variáveis serão correspondentes a ordem das Células Executadas</p>

# COMMAND ----------

print(test_variavel)

# COMMAND ----------

test_variavel = 'hello world'

# COMMAND ----------

# MAGIC %md ## Linguagens
# MAGIC
# MAGIC <p>O databricks é uma plataforma baseada em spark para processamento de dados. O processamento de dados pode ser feito em Python, Scala, R e SQL. Existe uma linguagem padrão do notebook configurada e as outras linguagens podem ser utilizadas com o comando mágico "%" e o nome da lingagem.</p>

# COMMAND ----------

print('hello world')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT 'hello world' AS saudacao

# COMMAND ----------

# MAGIC %md ## Arquitetura do Cluster

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![Arquitetura Spark](https://github.com/arturjacques/DatabricksCourse/blob/main/images/cluster-overview.png?raw=true)

# COMMAND ----------


