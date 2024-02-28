# Databricks notebook source
# MAGIC %md # DataFrame
# MAGIC
# MAGIC <p>
# MAGIC   DataFrames são estruturas de dados distribuídas em PySpark organizadas em linhas e colunas. Pode ser criado de várias formas como spark.read.{formato}, sendo que o formato pode ser json, csv, txt, parquet. Além disso conexões com banco de dados, kafka, eventhub podem ter como saída um dataframe também. Além disso é possível criar um dataframe com diretamente na célula com dados em listas ou dicionários.
# MAGIC </p>
# MAGIC
# MAGIC <p>
# MAGIC   Todas as transformações possíves de se realizar em SQL é possível também serem realizados nos dataframes. Há diversos métodos para tranformação dos dados que é possível ver em mais detalhes na documentação lincada como leitura adicional. 
# MAGIC </p>
# MAGIC
# MAGIC <p>
# MAGIC   É importante notar que o DataFrame é uma abstração dos dados. Dependendo do formato dos dados de origem ele funciona um pouco diferente no "back-end" se você está criando um DataFrame com base em um parquet e realizando o select de uma coluna, como o parquet é um formato coluna, o spark é capaz de na hora de realizar a leitura selecionar somente as colunas que você quer do arquivo e não ler o resto dele, aumentando muito o tempo de execução. No entando se você ta lendo um CSV e realiza um select de uma coluna o spark precisa ler o arquivo inteiro para retirar os dados que você precisa.
# MAGIC </p>
# MAGIC
# MAGIC <p>
# MAGIC   Alguns formatos de dados não possuem um schema definido como csv, json, txt. Para esses formatos é importante na hora da leitura dos dados fornecer o schema para garantir que os dados sejam lidos da forma correta, principalmente quando é realizado a leitura de vários arquivos ao mesmo tempo.
# MAGIC </p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html">https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrame.html</a></p>

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

# MAGIC %md
# MAGIC
# MAGIC ## Schema
# MAGIC
# MAGIC <p>
# MAGIC   Para forçar um schema quando cria uma tabela no spark é possível definir o schema durante a leitura do DataFrame. Arquivos diferentes podem ter sido salvos com os mesmos dados porém com tipos diferente por falta de consistência durante o processo de salvamento. No setup a baixo vamos salvar 2 arquivos um em que o valor é dado como um float e um como um integer. E vamos tentar ler esses dados.
# MAGIC </p>

# COMMAND ----------

# setup
compras_1 =  [
    {"id_compra": 101, "valor": 150.00, "produto": "Vestido", "id_pessoa": 1},
    {"id_compra": 102, "valor": 50.00, "produto": "Sapato", "id_pessoa": 1},
    {"id_compra": 103, "valor": 200.00, "produto": "Relógio", "id_pessoa": 2}
]

compras_2 = [
    {"id_compra": 104, "valor": 100, "produto": "Camisa", "id_pessoa": 2},
    {"id_compra": 105, "valor": 300, "produto": "Notebook", "id_pessoa": 3},
    {"id_compra": 106, "valor": 80, "produto": "Fone de ouvido", "id_pessoa": 3}
]

compras_1_df = (spark
    .createDataFrame(compras_1)
    .coalesce(1))

(compras_1_df
    .write
    .option("header",True)
    .mode('overwrite')
    .csv("/tmp/compras/compras_1.csv")
)

compras_2_df = (spark
    .createDataFrame(compras_2)
    .coalesce(1))

(compras_2_df
    .write
    .option("header",True)
    .mode('overwrite')
    .csv("/tmp/compras/compras_2.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Para obter o schema de um dataframe pode utilizar o método ".schema". Pode ser usado como base para construir o schema de leitura. Também é possível ler a primeira vez inferindo o schema e depois criar o schema de leitura.
# MAGIC </p>

# COMMAND ----------

compras_1_df.schema

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Quando foi criado o DataFrame o tipo do valor era "DoubleType" que é parecido com o "float" mas possui uma precisão maior. Porem quando lemos já sabemos que essa coluna é uma coluna de valor monetário, logo não terá mais de 2 casas decimais na maioria dos casos então podemos ler utilizando o tipo decimal(18, 2) que significa que ele tera 18 digitos com 2 deles podendo ser decimais.
# MAGIC   StructType é definido com um conjunto de colunas da tabela. O StructType recebe uma lista de colunas e cada campo da coluna é um StructField que possui 3 parametros o nome da coluna, o tipo de coluna e o terceiro é opcional se o valor da coluna pode ser nulo ou não. O StruckField pode receber também parametros como array ou outro StrucType ficando um grupo de colunas dentro de uma coluna.
# MAGIC </p>

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, LongType, IntegerType, DecimalType, StringType,MapType,ArrayType

schema = StructType(
    [
        StructField('id_compra', StringType(), True),
        StructField('id_pessoa', StringType(), True),
        StructField('produto', StringType(), True),
        StructField('valor', DecimalType(18,2), True)
    ]
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Lendo tanto o primeiro arquivo quanto o segundo notamos que o schema se mantem constante do dataframe. 
# MAGIC </p>

# COMMAND ----------

spark.read.format('csv').option("header",True).schema(schema).load("/tmp/compras/compras_1.csv").display()

# COMMAND ----------

spark.read.format('csv').option("header",True).schema(schema).load("/tmp/compras/compras_2.csv").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Se lermos os dois arquivos ao mesmo tempo utilizando um "wild card" que é o "*" sem passar o schema notamos que todas as colunas são lidas como string, por conta disso o segundo arquivo fica sem casas decimais e o primeiro com. Os dados podem ser convertidos depois para tipo "Decimal(18,2)". Quando lidamos com o recebimento de dados em uma camada bronze geralmente lemos todas as colunas como string para evitar problemas de conversão e só convertemos eles para a camada silver.
# MAGIC </p>

# COMMAND ----------

spark.read.format('csv').option("header",True).load("/tmp/compras/*").display()

# COMMAND ----------

# MAGIC %md ## Temporary View
# MAGIC
# MAGIC <p>Views temporarias são uma maneira de converter dataframes para tabelas para utilizar comando SQL. Importante notar que essas views criadas só existem no contexto do notebook, dessa forma outro desenvolvedor executando alguma alteração em outro notebook não será capaz de ver essa view. Também essa view desaparece se o cluster for desligado (funciona como se fosse uma variável em python).</p>
# MAGIC <p>Para criar essa view pode ser utilizado o comando SQL "CREATE TEMP VIEW AS SELECT" ou pode utilizar o método "createTempView" ou "createOrReplaceTempView" de um dataframe para transforma-lo em uma view temporária.</p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-view">https://learn.microsoft.com/pt-br/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-view</a></p>

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
# MAGIC <p>O Comando display pode ser utilizado de duas formas. No inicio como função e no final como método do objeto DataFrame. Também há o método "Show" que mostra os dados em forma de texto. Esse comando é uma action, então quando executado força que as transformações sejam feitas. O comando display possui um limite de linhas que ele ira mostrar, pode varia dependendo dos arquivos que ele abriu para mostrar. Quando se está lendo uma tabela com um milhão de linhas ele só irá abrir os primeiros arquivos e mostrar na tela.</p>

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
# MAGIC <p>
# MAGIC O comando select permite que sejam selecionada colunas de uma tabela, também pode utilizar alias ou realizar soma de colunas (assim como em SQL).
# MAGIC </p>

# COMMAND ----------

help(df_pessoas.select)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT nome, idade anos_desde_o_nascimento FROM pessoas_table

# COMMAND ----------

df_pessoas.display()

# COMMAND ----------

from pyspark.sql.functions import col

df_pessoas.select('nome', 'idade', (col("idade")+col('id_pessoa')).alias('idade_mais_id')).display()

# COMMAND ----------

from pyspark.sql.functions import col

df_pessoas.select('nome', col('idade').alias("anos_desde_que_nasceu")).display()

# COMMAND ----------

# MAGIC %md ##WithColumn
# MAGIC
# MAGIC <p>O comando withColumn permite que sejam feitas novas colunas ou alteradas colunas existentes. Esse comando não é utilizado para agrupar colunas, somente para operações narrow. É possível realizar contas, concatenar colunas, tratar textos, verificar certas condições para ter um retorno boleano.</p>

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
# MAGIC <p>O comando withColumnRenamed é utilizado para renomar uma coluna, primeiro valor é o nome antigo da coluna e o segundo o novo nome.</p>

# COMMAND ----------

help(df_compras.withColumnRenamed)

# COMMAND ----------

df_compras.withColumnRenamed("id_pessoa", "id_user").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Join
# MAGIC
# MAGIC <p>O comando de join é utilizado para juntar dados de duas tabelas na mesma tabela por uma chave em comum, a maneira que o join no pyspark tem a mesma lógica do que em SQL.</p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://www.devmedia.com.br/sql-join-entenda-como-funciona-o-retorno-dos-dados/31006">https://www.devmedia.com.br/sql-join-entenda-como-funciona-o-retorno-dos-dados/31006</a></p>
# MAGIC
# MAGIC ![Widget Location](https://arquivo.devmedia.com.br/artigos/Fernanda_sallai/sql_join/image001.jpg)
# MAGIC
# MAGIC <p>Fonte: <a href="https://www.devmedia.com.br/sql-join-entenda-como-funciona-o-retorno-dos-dados/31006">https://www.devmedia.com.br/sql-join-entenda-como-funciona-o-retorno-dos-dados/31006</a></p>

# COMMAND ----------

help(df_pessoas.join)

# COMMAND ----------

(df_pessoas
 .alias('pessoas')
 .join(
     df_videogames_favoritos.alias("videogame"), 
     ['id_pessoa'], 
     'left')
 .select("pessoas.id_pessoa", "videogame.videogame")
 ).display()

# COMMAND ----------

# MAGIC %md ##GroupBy
# MAGIC
# MAGIC <p>GroupBy permite agrupar as linhas que possuem colunas iguais, pode se agrupar por uma coluna só ou por várias ao mesmo tempo.</p>

# COMMAND ----------

help(df_compras.groupBy)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Um exemplo é agrupar pelo id do usuário para obter informações resumidas sobre esse usuário. As colunas que serão formadas precisam ter funções que considerem agregações como count_distinct (verifica quantos valores diferentes tem naquela coluna para aquele grupamento), max (valor máximo do valor da coluna para o grupamento), min (valor minimo do valor ou data da coluna para o grupamento), sum (soma dos valores para aquela coluna naquele grupamento), count (conta quantos valores tem naquela coluna para aquele grupamento), avg (média entre os valores da coluna para aquele grupamento) e etc.
# MAGIC </p>

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
