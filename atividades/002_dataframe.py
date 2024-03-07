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
# MAGIC <p>Faça uma tabela com o id das pessoas, quantas vezes cada pessoa fez compra (considerando que o mesmo valor de "compra_id" é uma compra só), items total comprados, items diferentes comprados, quanto foi o item mais caro comprado em uma compra.</p>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 5
# MAGIC
# MAGIC <p>
# MAGIC     Crie uma tabela fazendo join da tabela customer com a tabela nation para obter o país do usuário no schema 'samples.tpch'.
# MAGIC </p>

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Desafio
# MAGIC
# MAGIC <p>O teste unitário a baixo valida uma função que sumariza um dataframe contendo para cara "id_pessoa" a quantidade total de compras "compras_qt" e o valor total comprado "valor_total". Escreva a função que recebe um dataframe e retorna um dataframe que passe no teste a baixo e a use para processar df e salvar em uma variável chamada df_processed.</p>

# COMMAND ----------

from pyspark.sql.functions import count, sum

def test_clientes_sumarizados():
    #prepare
    data = [
        {"compra_id": 1, "id_pessoa": 1, "id_produto": 1, "valor":50},
        {"compra_id": 1, "id_pessoa": 1, "id_produto": 2, "valor":100},
        {"compra_id": 2, "id_pessoa": 2, "id_produto": 1, "valor":75},
    ]

    resultado_esperado = [
        {"id_pessoa": 1, "compras_qt": 2, "valor_total":150},
        {"id_pessoa": 2, "compras_qt": 1, "valor_total":75} 
    ]

    df = spark.createDataFrame(data)

    #call
    df_returned = clientes_sumarizados(df)

    #assert
    df_collect = df_returned.collect()
    resultado_retornado = [row.asDict() for row in df_collect]

    assert resultado_esperado == resultado_retornado, f"os dados não estão iguais {resultado_esperado} e {resultado_retornado}"

# COMMAND ----------

def clientes_sumarizados(df):
    return #write here

test_clientes_sumarizados()
