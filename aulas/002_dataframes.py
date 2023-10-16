# Databricks notebook source
# MAGIC %md # DataFrame
# MAGIC
# MAGIC <p>DataFrames são estruturas de dados distribuídas em PySpark organizadas em linhas e colunas</p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <h2>SETUP</h2>

# COMMAND ----------

pessoas = [
    {"id_pessoa": 1, "nome": "Ana", "idade": 25, "sexo": "F"},
    {"id_pessoa": 2, "nome": "Bruno", "idade": 30, "sexo": "M"},
    {"id_pessoa": 3, "nome": "Carla", "idade": 28, "sexo": "F"}
]

compras =  [
        {"id_compra": 101, "valor": 150.00, "produto": "Vestido", "id_pessoa": 1},
        {"id_compra": 102, "valor": 50.00, "produto": "Sapato", "id_pessoa": 1},
        {"id_compra": 103, "valor": 200.00, "produto": "Relógio", "id_pessoa": 2},
        {"id_compra": 104, "valor": 100.00, "produto": "Camisa", "id_pessoa": 2},
        {"id_compra": 105, "valor": 300.00, "produto": "Notebook", "id_pessoa": 3},
        {"id_compra": 106, "valor": 80.00, "produto": "Fone de ouvido", "id_pessoa": 3}
    ]

videogames_favoritos = [ 
    {"id_pessoa": 1, "console": "PlayStation 5", "videogame": "God of War: Ragnarok"},
    {"id_pessoa": 2, "console": "Xbox Series X", "videogame": "Halo Infinite"},
    {"id_pessoa": 3, "console": "Nintendo Switch", "videogame": "Animal Crossing: New Horizons"} 
]

df_pessoas = spark.createDataFrame(pessoas)
df_compras = spark.createDataFrame(compras)
df_videogames_favoritos = spark.createDataFrame(videogames_favoritos)

# COMMAND ----------

# MAGIC %md ## Temporary View
# MAGIC
# MAGIC <p>Views temporarias são uma maneira de converter dataframes para tabelas para utilizar comando SQL.</p>

# COMMAND ----------

help(df_pessoas.createOrReplaceTempView)

# COMMAND ----------

df_pessoas.createOrReplaceTempView("pessoas_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM pessoas_table

# COMMAND ----------

spark.sql("SELECT * FROM pessoas_table").display()

# COMMAND ----------

# MAGIC %md ##Comando Display
# MAGIC
# MAGIC <p>O Comando display pode ser utilizado de duas formas. No inicio como função e no final como método do objeto DataFrame. Também há o método "Show" que mostra os dados em forma de texto.</p>

# COMMAND ----------

help(display)

# COMMAND ----------

display(df_pessoas)

# COMMAND ----------

df_compras.display()

# COMMAND ----------

df_compras.show()

# COMMAND ----------

# MAGIC %md ## Select
# MAGIC
# MAGIC O comando select permite que sejam selecionada colunas de uma tabela.

# COMMAND ----------

help(df_pessoas.select)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT nome, idade anos_desde_o_nascimento FROM pessoas_table

# COMMAND ----------

df_pessoas.select('nome', 'idade').display()

# COMMAND ----------

from pyspark.sql.functions import col

df_pessoas.select('nome', col('idade').alias("dias_desde_que_nasceu")).display()

# COMMAND ----------

# MAGIC %md ##WithColumn
# MAGIC
# MAGIC <p>O comando withColumn permite que sejam feitas novas colunas ou alteradas colunas existentes.</p>

# COMMAND ----------

help(df_compras.withColumn)

# COMMAND ----------

dolar_para_real = 1/5

df_compras.withColumn("valor_em_dolar", col('valor')*dolar_para_real).display()

# COMMAND ----------

#como o retorno dos métodos é um objeto dataframe é possível utilizar vários comando seguidos
from pyspark.sql.functions import lit

(df_compras
    .select('id_pessoa', 'valor')
    .withColumn('valor_em_dolar', col('valor')*dolar_para_real)
    .select('id_pessoa', 'valor_em_dolar')
    .withColumn("unidade", lit("loja_querida"))
).display()

# COMMAND ----------

# MAGIC %md ##WithColumnRenamed
# MAGIC <p>O comando withColumnRenamed é utilizado para renomar uma coluna.</p>

# COMMAND ----------

help(df_compras.withColumnRenamed)

# COMMAND ----------

df_compras.withColumnRenamed("id_pessoa", "id_user").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Join
# MAGIC
# MAGIC <p>O comando de join é utilizado para juntar dados de duas tabelas na mesma tabela por uma chave em comum.</p>

# COMMAND ----------

help(df_pessoas.join)

# COMMAND ----------

df_pessoas.join(df_videogames_favoritos, ['id_pessoa'], 'left').display()

# COMMAND ----------

# MAGIC %md ##GroupBy
# MAGIC
# MAGIC <p>GroupBy permite agrupar as linhas por caracteristicas e fazer contas.</p>

# COMMAND ----------

help(df_compras.groupBy)

# COMMAND ----------

from pyspark.sql.functions import max, min, count_distinct, sum, avg, count

df_compras.groupBy('id_pessoa').agg(
    count_distinct('produto').alias("produtos_diferentes_comprados"),
    max("valor").alias("maximo_valor_pago_por_um_produto"),
    min("valor").alias("minimo_valor_pago_por_um_produto"),
    sum("valor").alias("valor_total_comprada"),
    count("produto").alias("quantidade_total_comprada"),
    avg("valor").alias("media_de_valor_por_produto")
).display()

# COMMAND ----------

# MAGIC %md ##Salvando a DataFrame
# MAGIC
# MAGIC <p>Iniciamente vamos criar uma tabela salvando com overwrite completo da tabela.</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- criando schema
# MAGIC CREATE SCHEMA IF NOT EXISTS dataframe_aula 

# COMMAND ----------

df_pessoas.write.mode("overwrite").saveAsTable("dataframe_aula.pessoas")
df_compras.write.mode("overwrite").saveAsTable("dataframe_aula.compras")

# COMMAND ----------

# MAGIC %md #Alterando Tabela
# MAGIC

# COMMAND ----------

# MAGIC %md ## Comentários
# MAGIC
# MAGIC <p>Comentários devem ser adicionados a tabela e colunas para aumentar o compreendimento dos usuários</p>

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE dataframe_aula.pessoas

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COMMENT ON TABLE dataframe_aula.pessoas IS "tabela contendo dados sobre os clientes da lojas";
# MAGIC ALTER TABLE dataframe_aula.pessoas CHANGE id_pessoa id_pessoa bigint COMMENT "identificado do cliente";
# MAGIC ALTER TABLE dataframe_aula.pessoas CHANGE idade idade bigint COMMENT "idade do cliente";
# MAGIC ALTER TABLE dataframe_aula.pessoas CHANGE nome nome string COMMENT "nome do cliente (PII)";
# MAGIC ALTER TABLE dataframe_aula.pessoas CHANGE sexo sexo string COMMENT "sexo do cliente";

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE dataframe_aula.pessoas
