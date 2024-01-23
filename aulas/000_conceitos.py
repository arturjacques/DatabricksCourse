# Databricks notebook source
# MAGIC %md #Conceitos básicos do Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Comandos Básicos
# MAGIC
# MAGIC ## Execução
# MAGIC
# MAGIC <p>O Databricks Utiliza um notebook para executar o código. As células serão executadas em sequência quando for utilizado o botão <b>Run all</b>, mas quando executando iterativamente a execução das celulas fica a critério do programador. Os dados que ficarão em memoria Sobre variáveis serão correspondentes a ordem das Células Executadas</p>

# COMMAND ----------

#execute essa célula segundo

print(test_variavel)

# COMMAND ----------

#execute essa célula primeiro

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

# MAGIC %md
# MAGIC
# MAGIC ## Transformações e Ações
# MAGIC
# MAGIC <p>Transformações são comandos que realizam modificação dos dados select, filter, join, groupBy, union e ETC. Esses comandos são armazenados e não são realizados até que uma ação seja realizada. Esse tipo de comportamento é chamado de "lazy".</p>
# MAGIC
# MAGIC <p>Ações são comandos que exigem que os dados sejam processados como count, display, save. Esses comandos para executarem corretamente existem que as transformações ocorram e que o plano de execução aconteça.</p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://www.linkedin.com/pulse/spark-transformations-actions-lazy-evaluation-mohammad-younus-jameel">https://www.linkedin.com/pulse/spark-transformations-actions-lazy-evaluation-mohammad-younus-jameel</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ###exemplo

# COMMAND ----------

#setup criando os dados

lista_compras = [
    {"id_compra": 1, "id_usuario": 101, "valor": 50.0, "imposto": 5.0, "data": "2022-01-01"},
    {"id_compra": 2, "id_usuario": 102, "valor": 30.0, "imposto": 3.0, "data": "2022-01-02"},
    {"id_compra": 3, "id_usuario": 103, "valor": 100.0, "imposto": 10.0, "data": "2022-01-03"},
    {"id_compra": 4, "id_usuario": 104, "valor": 80.0, "imposto": 8.0, "data": "2022-01-04"},
    {"id_compra": 5, "id_usuario": 105, "valor": 120.0, "imposto": 12.0, "data": "2022-01-05"}
]

lista_usuarios = [
    {"id_usuario": 101, "nome": "João", "idade": 25, "cidade": "São Paulo"},
    {"id_usuario": 102, "nome": "Maria", "idade": 30, "cidade": "Rio de Janeiro"},
    {"id_usuario": 103, "nome": "Pedro", "idade": 28, "cidade": "Belo Horizonte"},
    {"id_usuario": 104, "nome": "Ana", "idade": 27, "cidade": "Curitiba"},
    {"id_usuario": 105, "nome": "Lucas", "idade": 35, "cidade": "Porto Alegre"}
]

df_compras = spark.createDataFrame(lista_compras)
df_usuario = spark.createDataFrame(lista_usuarios)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Transformação
# MAGIC
# MAGIC <p>Executando a célula a baixo é possível notar que a célula executa muito rápido e não há nenhum spark job. Isso porque só foram salvas as alterações que devem ser feitas, o spark só realiza uma validação para verificar se o plano de execução que você montou é possível de realizar.</p>

# COMMAND ----------

#transformações

df_compras_select = df_compras.select("id_usuario", "valor")
df_usuario_select = df_usuario.select("id_usuario", "nome", "idade")

df_join = (df_compras_select
    .alias("compras")
    .join(
        df_usuario_select.alias("usuario"),
        ['id_usuario'],
        'left'
    )
)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Ação
# MAGIC
# MAGIC <p>Executando as células a baixo é possível verificar que o há uma demora maior para execução e há a execução de spark jobs. Isso porque as tranformações planejadas foram feitas para realizar o display e o count. Também é possível ver que há o mesmo número de jobs nas duas execuções. O spark não mantem em memoria os resultados anteriores tendo que realizar todas as transformações em cada ação.</p>

# COMMAND ----------

#action

df_join.display()

# COMMAND ----------

#action

df_join.count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p>É possível verificar qual é o plano montado para um dataframe utilizando o método explain que exibe o plano formado para as transformações solicitadas.</p>

# COMMAND ----------

df_join.explain(mode='simple')

# COMMAND ----------

# MAGIC %md ## Arquitetura do Cluster

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![Arquitetura Spark](https://arquivo.devmedia.com.br/artigos/Eduardo_Zambom/spark/image002.jpg)
# MAGIC
# MAGIC <p>Fonte: <a href="https://www.devmedia.com.br/introducao-ao-apache-spark/34178">https://www.devmedia.com.br/introducao-ao-apache-spark/34178</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p><b>Driver Program</b>: este é o entry point do Spark. É onde o Spark Context é criado e é onde se define o fluxo de execução, bem como o RDD e o que deve ser executado em paralelo pelos Executores.</p>
# MAGIC
# MAGIC <p><b>Spark Context</b>: Estabelece configurações de memória e processamento dos Workers Nodes. Além disso é capaz de conectar com os diferentes tipos de Cluster Manager (além do próprio Spark Cluster Manager) como Apache Mesos ou Yarn do Hadoop.</p>
# MAGIC
# MAGIC <p><b>Cluster Manager</b>: esse é responsável por agendar e alocar um recurso computacional através do cluster.</p>
# MAGIC
# MAGIC <p><b>Worker Node</b>: é uma máquina que contém executores e recebe as tasks do Driver Program.</p>
# MAGIC
# MAGIC <p><b>Executor</b>: é o responsável por executar as tasks.</p>
# MAGIC
# MAGIC <p><b>Task</b>: é qualquer conjunto de operações (select, joing, filter, algorítimo de machine learning, e.t.c) sobre os dados.</p>
# MAGIC
# MAGIC <p>Fonte: <a href="https://lorenadesouza.medium.com/bootcamp-de-dados-na-tw-spark-6633275480e4">https://lorenadesouza.medium.com/bootcamp-de-dados-na-tw-spark-6633275480e4</a></p>

# COMMAND ----------

# MAGIC %md ## Spark Stages 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![Spark Stages](https://media.licdn.com/dms/image/D4D12AQFvdf6KAsGdiA/article-cover_image-shrink_423_752/0/1682015360609?e=1709769600&v=beta&t=Q1Th_DpzOBGD_1p5WozGv5cWMkDOv3GUzSCBDlGeqL0)
# MAGIC
# MAGIC <p>Fonte: <a href="https://www.linkedin.com/pulse/wide-vs-narrow-transformations-sparkdistributed-compute-don-hilborn/">https://www.linkedin.com/pulse/wide-vs-narrow-transformations-sparkdistributed-compute-don-hilborn/</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC <p><b>Narrow Stages</b>: são estágios onde os dados não precisam ser organizados para que a execução funciona. cada execução trabalha em um parte dos dados principais. Narrow Stages são executados em paralelo.</p>
# MAGIC
# MAGIC <p><b>Wide Stages</b>: São estágios em que o dado precisa estar organizado entre os workers nodes do cluster. Cada Wide Stage precisa operar em todas as partições do RDD. Wides Stages normalmente demandam mais poder computacional que operações Narrow</p>
# MAGIC
# MAGIC <p>Fonte: <a href="https://sparkbyexamples.com/spark/what-is-spark-stage/">https://sparkbyexamples.com/spark/what-is-spark-stage/</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### exemplo
# MAGIC
# MAGIC <p>Imagine que você e mais 3 amigos precisam organizar um baralho de cartas e deixar eles separados por naipe em ordem crescente. Como seria possível que todos trabalhassem ao mesmo tempo para executar essa tarefa?</p>
# MAGIC
# MAGIC <p>Uma forma possível seria dividir o baralho inteiro em 4 partes e cada um ficar com 1/4 do baralho. Agora com todos com o baralho em mãos a função de cada um é separar a sua parte em 4 grupos, um para cada naipe.</p>
# MAGIC
# MAGIC <p>Com cada naipe separado para cada uma das partes do baralho agora seria entregue um naipe para cada um dos integrantes. Dessa forma cada um dos 3 amigos precisa entregar copas pro primeiro, depois 3 pessoas entregam espadas para o segundo e assim por diante.</p>
# MAGIC
# MAGIC <p>Cada um tendo somente cartas de um naipe agora eles podem colocar o baralho em ordem e ao final juntar os baralhos.</p>
# MAGIC
# MAGIC <p>O primeiro estágio é narrow uma vez que o baralho não precisou ser organizado para a execução, era somente um filtro simples de naipe. Já a segunda operação necessitava que as cartas fossem entregues para participantes diferentes provocando um "shuffle" para que assim a tarefa fosse realizada e por isso ela é Wide.</p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![Arquitetura Spark](https://cdnm.westwing.com.br/glossary/uploads/br/2022/06/07164208/baralho-completo-pixabay-c-a937.png)
# MAGIC
# MAGIC <p>Fonte: <a href="https://cdnm.westwing.com.br/glossary/uploads/br/2022/06/07164208/baralho-completo-pixabay-c-a937.png">https://cdnm.westwing.com.br/glossary/uploads/br/2022/06/07164208/baralho-completo-pixabay-c-a937.png</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Spark Job, Stage e Task
# MAGIC
# MAGIC <p>Seguindo o exemplo o job seria a ação por completo de organizar o baralho por naipe depois redistribuir entre as pessoas e organizar em ordem crescente cada naipe. O stage seria uma parte executado em paralelo como o primeiro stage que é o de separação por naipe e o segundo de organização em ordem crescente. Já a task é a tarefa em sí cada uma da pessoas organizou uma parte do baralho por naipe no primeiro stage totalizando 4 tarefas (uma para cada um) e depois organizou em ordem crescente com mais 4 tarefas.</p>
# MAGIC
# MAGIC <p><b>Formalizando</b></p>
# MAGIC
# MAGIC <p><b>Job</b>: é o conjunto de transformações executado nos dados.</p>
# MAGIC
# MAGIC <p><b>Stage</b>: é o conjunto de operações que podem ser executado em pararelo sem reorganização dos dados. O limite do 'stage' vai ser definido quando é necessário transferir dados entre nós para continuar a execução</p>
# MAGIC
# MAGIC <p><b>Task</b>: é a menor unidade de trabalho executada. Um 'stage' é composto por várias task distribuídas entre diferentes máquinas.</p>
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://medium.com/@diehardankush/what-are-job-stage-and-task-in-apache-spark-2fc0d326c15f">https://medium.com/@diehardankush/what-are-job-stage-and-task-in-apache-spark-2fc0d326c15f</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Medallion Architecture
# MAGIC
# MAGIC <p>
# MAGIC   A arquitetura medalion separa os dados em 3 camadas bronze, silver e gold.
# MAGIC </p>
# MAGIC
# MAGIC 1. Bronze: recebe o dado cru como obtido na fonte, os dados geralmente são salvos como string e somente apendados em uma tabela na camada bronze, essa camada não é para analises ela serve somente como um ponto de recebimento de dados que serão processados.
# MAGIC 2. Silver: ela tem como fonte a camada bronze mas tem durante o processo de alimentar as tabelas silver é removido duplicados e os dados são limpos.
# MAGIC 3. Gold: Uma camada de agregação com regras de negocio. Nessa camada que estão as tabelas que serão fonte para o BI.
# MAGIC
# MAGIC <p>Leitura adicional: <a href="https://www.databricks.com/glossary/medallion-architecture">https://www.databricks.com/glossary/medallion-architecture</a></p>

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![Arquitetura Spark](https://cms.databricks.com/sites/default/files/inline-images/building-data-pipelines-with-delta-lake-120823.png)
# MAGIC
# MAGIC <p>Fonte: <a href="https://www.databricks.com/glossary/medallion-architecture">https://www.databricks.com/glossary/medallion-architecture</a></p>
