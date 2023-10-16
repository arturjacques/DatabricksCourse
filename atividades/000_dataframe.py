# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Exercicio

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Setup

# COMMAND ----------

from functions.data_creation import create_compras_de_usuario

df = create_compras_de_usuario(spark)

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 1
# MAGIC
# MAGIC <p>Crie uma view temporaria do dataframe "df" com o nome "compras" e execute um "SELECT" em SQL.</p>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 2
# MAGIC
# MAGIC <p>Adicione uma coluna nova no dataframe "df" com a data de hoje no modelo "yyyy-mm-dd" e salve na variável df_with_metadata.</p>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 3
# MAGIC
# MAGIC <p>Troque o nome da coluna "valor" do dataframe "df" para "valor_usd" e salve em uma variável chamada "df_renamed".</p>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 4
# MAGIC
# MAGIC <p>Faça uma tabela com o id da pessoas, quantas vezes cada pessoa fez compra (considerando que o mesmo valor de "compra_id" é uma compra só), items total comprados, items diferentes comprados, quanto foi o item mais caro comprado em uma compra.</p>

# COMMAND ----------


