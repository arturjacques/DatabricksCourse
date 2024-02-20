# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Streaming de Dados
# MAGIC
# MAGIC <p>
# MAGIC   Operações de dados em streaming é uma ferramenta muito útil para diversas fontes de dados. Dados de diversas origens podem ser lidos utilizando streaming, fontes como kafka, event hub, storage account e tabelas deltas podem ser conectadas diretamente no databricks e serem consumidas. Fonte de dados em arquivos podem ser lidos utilizando auto loader e transformados em uma fonte de dados streaming que toda vez que há uma arquivo novo em determinado diretório o arquivo é processado, esse processo pode ser usado mesmo quando se deseja operações batch.
# MAGIC </p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##SETUP

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

SCHEMA = ALUNO + "_schema"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

print(30*'=')
print(f"CREATE USER SCHEMA {SCHEMA}")
print(30*'=')

# COMMAND ----------

from functions.data_creation import create_compras_de_usuario_list

diretorio_aluno = f"/tmp/{ALUNO}/"
diretorio_comrpas = diretorio_aluno + "compras"

dbutils.fs.rm(diretorio_aluno, True)
dbutils.fs.mkdirs(diretorio_aluno)

# COMMAND ----------

dado_fornecedor = [
    {'id_product': 0, 'id_fornecedor': 100},
    {'id_product': 1, 'id_fornecedor': 101},
    {'id_product': 2, 'id_fornecedor': 102},
    {'id_product': 3, 'id_fornecedor': 103},
    {'id_product': 4, 'id_fornecedor': 104},
    {'id_product': 5, 'id_fornecedor': 105},
    {'id_product': 6, 'id_fornecedor': 106},
    {'id_product': 7, 'id_fornecedor': 107},
    {'id_product': 8, 'id_fornecedor': 108},
    {'id_product': 9, 'id_fornecedor': 109},
    {'id_product': 10, 'id_fornecedor': 110}
]

fornecedor_table_name = f"{SCHEMA}.cadastro_fornecedor"
spark.createDataFrame(dado_fornecedor).write.mode("overwrite").saveAsTable(fornecedor_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Salvando dados na storage para serem processado
# MAGIC
# MAGIC <p>
# MAGIC   Essa etapa geralmente é feita por outra ferramenta que adiciona os dados na storage que serão consumidos.
# MAGIC </p>

# COMMAND ----------

from functions.data_creation import create_compras_de_usuario_list
import json
from uuid import uuid4

def create_files_with_data(num_of_files, dbfs_directory):
    dbutils.fs.mkdirs(dbfs_directory)
    for i in range(num_of_files):
        compras_list = create_compras_de_usuario_list()

        with open(f"/dbfs/{dbfs_directory}/{uuid4()}.json", "w", encoding='utf8') as f:
            json.dump(compras_list, f, ensure_ascii=False)

create_files_with_data(3, diretorio_comrpas)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Bronze: Autoloader
# MAGIC
# MAGIC <p>
# MAGIC   Para realizar transformações diretamente em um processo streaming é necessário que essas transformações sejam narrow (ou seja, que agem somente na coluna) para que o processo possa acontecer, joins, groupBy e union só irão funcionar se a operação for em micro batch.
# MAGIC </p>
# MAGIC <p>
# MAGIC   Para utilizar o autoloader é necessário utilizar o spark.readStream com o formato cloudFiles, e pode conter a opção de schemaLocation que irá salvar o schema dos arquivos lidos em caso de ele mude com o tempo seja mantido os schemas. Também precisa passar o cloudFiles.format que é o formato do arquivo que sera lido e no load é o local onde os arquivos estão.
# MAGIC </p>
# MAGIC <p>
# MAGIC   Os processos nesse notebook utilizarão a trigger "availableNow" que faz com que o processo rode como um batch. Ele consome todos os dados disponíveis e para e na próxima vez que for executado ele olha o checkpoint e não lê novamente os arquivos que já foram consumidos anteriormente. Existem várias opções de trigger que podem ser lidar no material adicional.
# MAGIC </p>
# MAGIC
# MAGIC <p>Leitura adicional sobre trigger: <a href="https://docs.databricks.com/en/structured-streaming/triggers.html">https://docs.databricks.com/en/structured-streaming/triggers.html</a></p>

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql import DataFrame

def add_metadata_table(df: DataFrame, data_source)-> DataFrame:
    return (df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("batch_id", lit(str(uuid4())))
        .withColumn("input_file_name", input_file_name())
        .withColumn("data_source", lit(data_source))
    )

# COMMAND ----------

bronze_df = (spark.readStream \
                .format("cloudFiles")
                .option("cloudFiles.schemaLocation", diretorio_aluno+'/schema_table_bronze')
                .option("cloudFiles.format", "json")
                .load(diretorio_comrpas))

bronze_df_with_metadata = add_metadata_table(bronze_df, 'json_files')

stream = (bronze_df_with_metadata
            .writeStream
            .format("delta")
            .outputMode("append")
            .trigger(availableNow=True)
            .option("checkpointLocation", diretorio_aluno+'/table_checkpoint')
            .table(f"{SCHEMA}.bronze_compras"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Silver: Streaming
# MAGIC
# MAGIC <p>
# MAGIC   Na camada silver vamos ler de uma tabela delta bronze utilizando readStream. Não será mais utilizado autoloader e sim o streaming de uma tabela delta, para esse streaming funcionar corretamente o ideal é que a tabela anterior só receba append porque dessa forma a segunda etapa roda coletando somente os dados novos. Operações como update ou delete na tabela bronze causará o processo falhar porque o spark não saberá quais dados são novos e os antigos, para esses casos que é necessário update e delete o ideal é utilizar change data feed.
# MAGIC </p>
# MAGIC <p>
# MAGIC   Pela origem ser uma tabela delta não é necessário fornecer schema location porque a tabela delta tem o schema definido, por padrão quando é utilizado readStream é considerado que a tabela é Delta então não precisa me fornecer o formato, mas é importante fornecer o checkpoint location que vai ser responsável por manter os dados que já foram lidos. Caso seja necessário realizar um full load da tabela é necessário deleter o checkpoint.
# MAGIC </p>
# MAGIC
# MAGIC <p>Leitura adicional sobre change data feed: <a href="https://docs.databricks.com/en/delta/delta-change-data-feed.html">https://docs.databricks.com/en/delta/delta-change-data-feed.html</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Para processar a tabela silver poderiamos realizar somente operações narrow como na etapa bronze ou utilizar a técnica de micro batch usando a opção de for each batch. Para isso é necessário fazer uma função que receba um batch_id que é o id do batch e o batch_df que é o dataframe com os dados. Essa função deve realizar todas as transformações e o salvamento, para isso utilizaremos uma classe python, é uma boa ideia testar essa class com um batch dos dados.
# MAGIC </p>
# MAGIC <p>
# MAGIC   Vamos realizar um join do produto_id com uma tabela que tem o id do fornecedor de cada produto para adicionar mais dados na silver. Vamos persistir essa tabela em memoria para que não seja necessário ler a tabela novamente a cada novo batch em caso de streaming.
# MAGIC </p>

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType, TimestampType, BooleanType, FloatType, DecimalType
from pyspark.sql.functions import from_json, col, explode, posexplode

df = spark.table(f"{SCHEMA}.bronze_compras")

class ProcessBronzeBatch:
    def __init__(self, fornecedor_table_name, data_sink):
        self.df_fornecedor = spark.table(fornecedor_table_name).persist()
        self.df_fornecedor.count() # necessário para carregar a tabela
        self.data_sink = data_sink

    def _treat_compras(self, batch_df):
        schema = ArrayType(
                    StructType(
                        [
                            StructField("id_product", StringType()),
                            StructField("nome_do_produto", StringType()),
                            StructField("preço", DecimalType(16, 2)),
                            StructField("peso", DecimalType(16, 2))
                        ]
                    )
                )
        
        return (batch_df
                .withColumn(
                    "compras",
                    from_json(col('compras'),
                    schema)
                    )
                )
        
    def _explode_and_melt_compras_column(self, batch_df):
        df_compras_separeted = (batch_df
                                .select("*", 
                                        explode("compras").alias("compras_exploded")
                                        )
                                ).drop("compras")
        
        return df_compras_separeted.select("*", "compras_exploded.*").drop('compras_exploded')
    
    def _join_fornecedor_table(self, batch_df):
        return (batch_df
                    .join(self.df_fornecedor, ['id_product'], 'left')
                )
        
    def _save_table(self, batch_df):
        batch_df.write.mode("append").saveAsTable(self.data_sink)
        
    def process_bronze_table(self, batch_df, batch_id):
        batch_df_compras_treated = self._treat_compras(batch_df)
        batch_df_exploded_melt_compras = self._explode_and_melt_compras_column(batch_df_compras_treated)
        batch_df_join_fornecedor = self._join_fornecedor_table(batch_df_exploded_melt_compras)
        self._save_table(batch_df_join_fornecedor)

        
silver_table_batch = f"{SCHEMA}.silver_compras_batch"
ProcessBronzeBatch(fornecedor_table_name, silver_table_batch).process_bronze_table(df, '1')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>
# MAGIC   Uma vez que a função que está processando corretamente o batch é possível alterar a operação para realizar em streaming. Importante notar que qualquer operação de batch é possível realizar com micro batch, porem é importante notar que algumas operações podem demandar muito tempo e isso causará um atraso no streaming.
# MAGIC </p>

# COMMAND ----------

silver_table_streaming = f"{SCHEMA}.silver_compras"

process_obj = ProcessBronzeBatch(fornecedor_table_name, silver_table_streaming)

silver_df = (spark
            .readStream
            .table(f"{SCHEMA}.bronze_compras")
            )

stream =(silver_df
            .writeStream
            .foreachBatch(process_obj.process_bronze_table)
            .option("checkpointLocation", diretorio_aluno+'/table_silver_checkpoint')
            .trigger(availableNow=True)
            .start())
