# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC #Funções
# MAGIC
# MAGIC <p>
# MAGIC   A utilização de funções é uma importante ferramenta para processamento de dados. Funções garante um indice alto de reaproveitamento de código e consistência em todas as partes do processo, também permite que seja realizado testes unitários. Funções também são o primeiro passo para programação orientada o objeto que garante que sejam criados código genéricos que podem ser altamente parametrizados podendo usar somente um notebook para fazer extrações de várias fontes diferentes de dados.
# MAGIC </p>
# MAGIC
# MAGIC <p>Leitura adicional sobre python functions: <a href="https://www.programiz.com/python-programming/function">https://www.programiz.com/python-programming/function</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##SETUP
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Criação de um schema para o Aluno para criar isolamento entre as atividades. Dessa forma cada aluno salva em seu próprio schema chamando a variável "SCHEMA".
# MAGIC </p>

# COMMAND ----------

ALUNO = spark.sql("SELECT current_user() as user").collect()[0].user.split("@")[0]

if ALUNO == "":
    raise Exception("É necessário preencher o nome do aluno")
elif " " in ALUNO:
    raise Exception("O nome do aluno não pode conter espaço")

SCHEMA = "hive_metastore." + ALUNO + "_schema"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

print(30*'=')
print(f"CREATE USER SCHEMA {SCHEMA}")
print(30*'=')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Preparação do ambiente. Foi criada uma função que gera dados aleatórios de compras. Ele gera uma tabela com 5 colunas a compra_id que representa uma compra com vários items, compra_ts que é o timestamp da operação, id_pessoa que é o id do comprado, id_producto que é o id do produto e o valor do produto. Para obter a compra completa é necessário agrupar pela compra ID. A função pode receber 3 parametros, n_compras que é o número de compras que tera no dataframe, start_date a data de inicio dos dados, end_date data final dos dados.
# MAGIC </p>

# COMMAND ----------

from functions.data_creation import create_compras_de_usuario

df_data = create_compras_de_usuario(spark, n_compras=10, start_date='2024-01-01', end_date='2024-01-15')
df_data.write.saveAsTable(f"{SCHEMA}.raw_data_example")
df = spark.table(f"{SCHEMA}.raw_data_example")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dataframe In Dataframe Out
# MAGIC
# MAGIC <p>
# MAGIC   Esse tipo de função recebe um DataFrame e retorna um DataFrame possibilitando ser possível compartimentalizar ela e testar de forma adequada. Também pode receber parametros adicionais para realiza a alteração do DataFrame.
# MAGIC </p>
# MAGIC
# MAGIC <p>
# MAGIC   A função a baixo adiciona metadados a um dataframe de origem, muito útil para leitura de dados raw. São inseridas 4 novas colunas "ingestion_ts" que é o timestamp do momento da extração, batch_id um id único que é criado no momento da extração para identificar a extração, input_file_name que é o nome do arquivo que foi lido (porem nesse caso vai voltar vazio porque o dataframe é criado em memoria não lendo de um arquivo), data_source o nome da source do dado que é útil para quando a extração acontece de várias fontes diferentes e uma variável enviada para a função.
# MAGIC </p>

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit, col, coalesce, lit
from pyspark.sql import DataFrame
from uuid import uuid4

def add_metadata_table(df: DataFrame, data_source: str)-> DataFrame:
    return (df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("batch_id", lit(str(uuid4())))
        .withColumn("input_file_name", col("_metadata.file_name"))
        .withColumn("data_source", lit(data_source))
    )

add_metadata_table(df, "dados_criados_para_teste").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Void Functions
# MAGIC
# MAGIC <p>
# MAGIC   Funções sem retorno. São funções utilizdas para quando desejamos fazer orquestrações, salvamento de tabelas, roll back, alteração, setups e etc.
# MAGIC </p>

# COMMAND ----------

#função para realizar append de tabela

def append_table(df, table_name):
    df.write.mode('append').saveAsTable(table_name)

# COMMAND ----------

# função de orquestração 

def add_metadata_and_save_table(df, data_source, table_name):
    df_with_metadata = add_metadata_table(df, data_source)
    append_table(df_with_metadata, table_name)

# COMMAND ----------

add_metadata_and_save_table(df, "dados_criados_para_teste", f"{SCHEMA}.functions_table")

spark.table(f"{SCHEMA}.functions_table").display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Funções auxiliares
# MAGIC
# MAGIC <p>
# MAGIC   Essas funções muitas vezes não irão inserir dados em tabela diretamente mas são muito úteis para garantir um ambiente com governança e ter funções que facilitam a vida das pessoas que trabalham com dados.
# MAGIC </p>
# MAGIC
# MAGIC <p>
# MAGIC   a função a baixo retorna informações sobre as colunas da tabela, nome, tipo e comentários na forma de dicionário o que é mais fácil de lidar do que da forma de dataframe.
# MAGIC </p>

# COMMAND ----------

def get_table_columns_information_list(table_name):
    table_describe_collected = spark.sql(f"DESCRIBE {table_name}").collect()
    return [row.asDict() for row in table_describe_collected]

get_table_columns_information_list(f"{SCHEMA}.functions_table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   podemos criar uma função que adiciona diretamente um comentário em uma coluna passando o nome da tabela, nome da coluna e o tipo da coluna.
# MAGIC </p>

# COMMAND ----------

def set_column_commentary(table_name, col_name, col_type, comment):
    spark.sql(f"ALTER TABLE {table_name} CHANGE {col_name} {col_name} {col_type} COMMENT '{comment}'")

set_column_commentary(f"{SCHEMA}.functions_table", "compra_id", 'string', 'id da compra, ids repetidos pertence ao mesmo carrinho')

get_table_columns_information_list(f"{SCHEMA}.functions_table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Com as duas funções é possível criar uma função que permite que comente as colunas de uma tabela mandando um dicionário em que a chave é o nome da coluna e o valor o comentário. Essa função deve ter algumas caracteristicas, se for enviado uma coluna que não existe na tabela deve levantar um erro, ela deve aceitar comentários somente para algumas colunas e não todas e caso o comentário da coluna for igual a do dicionário enviado ela não deve executar a transformação.
# MAGIC </p>

# COMMAND ----------

def comment_table_based_on_dict(table_name, table_comment_dict):
    table_list_col = get_table_columns_information_list(table_name)
    col_dict_col = {}

    for column in table_list_col:
        col_name = column['col_name']
        col_dict_col[col_name] = column

    for column in column_comment_dict:
        if column in col_dict_col.keys():
            if column_comment_dict[column]!=col_dict_col[column]['comment']:
                set_column_commentary(table_name=table_name,
                                    col_name=column,
                                    col_type=col_dict_col[column]['data_type'],
                                    comment=column_comment_dict[column]
                                    )
        else:
            raise Exception(f"coluna {column} não existe na tabela {table_name}.")

column_comment_dict = {
    "compra_id": "id da compra pode ids repetidos pertence ao mesmo carrinho",
    "compra_ts": "data que foi efetuada a compra, é o mesmo valor para todas as compras com o mesmo compra_id",
    "id_pessoa": "id do usuário que fez a compra e faz join com a dimensão pessoas"
}

table_name = f"{SCHEMA}.functions_table"

comment_table_based_on_dict(table_name, column_comment_dict)

# COMMAND ----------

get_table_columns_information_list(f"{SCHEMA}.functions_table")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## LIMPEZA DO AMBIENTE DO USUARIO

# COMMAND ----------

spark.sql(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
