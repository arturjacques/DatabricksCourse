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

df.createOrReplaceTempView('compras')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM compras

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 2
# MAGIC
# MAGIC <p>Adicione uma coluna nova no dataframe "df" com a data de hoje no modelo "yyyy-mm-dd" e salve na variável df_with_metadata.</p>

# COMMAND ----------

from pyspark.sql.functions import current_date

df_with_metadata = df.withColumn('ingestion_date', current_date())

df_with_metadata.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 3
# MAGIC
# MAGIC <p>Troque o nome da coluna "valor" do dataframe "df" para "valor_usd" e salve em uma variável chamada "df_renamed".</p>

# COMMAND ----------

df_renamed = df.withColumnRenamed('valor', 'valor_usd')

df_renamed.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 4
# MAGIC
# MAGIC <p>Faça uma tabela com o id das pessoas, quantas vezes cada pessoa fez compra (considerando que o mesmo valor de "compra_id" é uma compra só), items total comprados, items diferentes comprados, quanto foi o item mais caro comprado em uma compra.</p>

# COMMAND ----------

from pyspark.sql.functions import count_distinct, count, max

(df
 .groupBy('id_pessoa')
 .agg(
    count_distinct("compra_id").alias("compras_feitas"),
    count("*").alias("items_totais_comprados"),
    count_distinct("id_product").alias("itens_diferentes_comprados"),
    max('valor').alias("valor_item_mais_caro_comprado")
 )
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exercicio 5
# MAGIC
# MAGIC <p>
# MAGIC     Crie uma tabela fazendo join da tabela customer com a tabela nation para obter o país do usuário no schema 'samples.tpch'.
# MAGIC </p>

# COMMAND ----------

from pyspark.sql.functions import col

df_customers = spark.table("samples.tpch.customer")
df_nation = spark.table("samples.tpch.nation")

(df_customers
 .alias('customer')
 .join(df_nation.alias('nation'), df_customers.c_nationkey == df_nation.n_nationkey, 'left')
 .select("customer.*", col("nation.n_name").alias("country_name"))
 ).display()

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
    return df.groupBy("id_pessoa").agg(count("*").alias("compras_qt"), sum("valor").alias("valor_total"))
  
test_clientes_sumarizados()

# COMMAND ----------

df_processed = clientes_sumarizados(df)

df_processed.display()

# COMMAND ----------


