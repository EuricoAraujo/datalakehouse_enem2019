import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min, max, lit

# Configuracao de logs de aplicacao
logging.basicConfig(stream=sys.stdout)
logger = logging.getLogger('datalake_enem_small_upsert')
logger.setLevel(logging.DEBUG)

# Definicao da Spark Session
spark = (SparkSession.builder.appName("DeltaEnem2019")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

logger.info("Importing delta.tables...")
from delta.tables import *


logger.info("Produzindo novos dados...")
enemnovo = (
    spark.read.format("delta")
    .load("s3://datalakehouse-eurico-enem2019/staging-zone/enem")
)

dados_select = enemnovo.select("CO_UF_RESIDENCIA",
                                "NU_IDADE",
                                "TP_SEXO",
                                "TP_ESTADO_CIVIL",
                                "TP_ESCOLA",
                                "IN_GESTANTE",
                                "TP_PRESENCA_CN",
                                "TP_PRESENCA_CH",
                                "TP_PRESENCA_LC",
                                "TP_PRESENCA_MT",
                                "NU_NOTA_CN",
                                "NU_NOTA_CH",
                                "NU_NOTA_LC",
                                "NU_NOTA_MT",
                                "NU_NOTA_REDACAO",
                                "Q006")
dados_select = dados_select.withColumnRenamed("NU_IDADE","IDADE").withColumnRenamed("TP_SEXO","SEXO").withColumnRenamed("TP_ESTADO_CIVIL","ESTADO_CIVIL").withColumnRenamed("TP_ESCOLA","ESCOLA").withColumnRenamed("IN_GESTANTE","GESTANTE").withColumnRenamed("TP_PRESENCA_CN","PRESENCA_CN").withColumnRenamed("TP_PRESENCA_CH","PRESENCA_CH").withColumnRenamed("TP_PRESENCA_MT","PRESENCA_MT").withColumnRenamed("NU_NOTA_CN","NOTA_CN").withColumnRenamed("NU_NOTA_CH","NOTA_CH").withColumnRenamed("NU_NOTA_LC","NOTA_LC").withColumnRenamed("NU_NOTA_MT","NOTA_MT").withColumnRenamed("NU_NOTA_REDACAO","NOTA_REDACAO").withColumnRenamed("Q006","RENDA_FAMILIA")
dados_filtrados = dados_select.filter((dados_select.IDADE > 16) & (dados_select.IDADE < 26) & dados_select.GESTANTE.isin(1) & dados_select.CO_UF_RESIDENCIA.isin(21,22,23,24,25,26,27,28,29))

dados_filtrados["CO_UF_RESIDENCIA"] = dados_filtrados.CO_UF_RESIDENCIA.replace({ 
    21 :"MA",
    22 :"PI",
    23 :"CE",
    24 :"RN", 
    25 :"PB", 
    26 :"PE", 
    27 :"AL", 
    28 :"SE",
    29 :"BA"
})

dados_filtrados["ESTADO_CIVIL"] = dados_filtrados.ESTADO_CIVIL.replace({ 
    0 :"N INFORMADO",
    1 :"SOLTEIRA",
    2 :"CASADA",
    3 :"DIVORCIADA", 
    4 :"VIUVA", 
})

dados_filtrados["ESCOLA"] = dados_filtrados.ESCOLA.replace({ 
    1 :"SEM RESPOSTA",
    2 :"PUBLICA",
    3 :"PRIVADA", 
    4 :"EXTERIOR", 
})

dados_filtrados["PRESENCA_CN"] = dados_filtrados.PRESENCA_CN.replace({ 
    0 :"FALTOU",
    1 :"PRESENTE",
    2 :"ELIMINADA"
})

dados_filtrados["PRESENCA_CH"] = dados_filtrados.PRESENCA_CH.replace({ 
    0 :"FALTOU",
    1 :"PRESENTE",
    2 :"ELIMINADA"
})

#dados_final["PRESENCA_LC"] = dados_final.PRESENCA_LC.replace({ 
#    0 :"FALTOU",
#    1 :"PRESENTE",
#    2 :"ELIMINADA"
#})

dados_filtrados["PRESENCA_MT"] = dados_filtrados.PRESENCA_MT.replace({ 
    0 :"FALTOU",
    1 :"PRESENTE",
    2 :"ELIMINADA"
})

dados_filtrados['RENDA_FAMILIA'] = dados_filtrados.RENDA_FAMILIA.replace({
    "A":	"Nenhuma renda",
    "B":	"Até R$ 998,00",
    "C":	"De R$ 998,01 até R$ 1.497,00",
    "D":	"De R$ 1.497,01 até R$ 1.996,00",
    "E":	"De R$ 1.996,01 até R$ 2.495,00",
    "F":	"De R$ 2.495,01 até R$ 2.994,00",
    "G":	"De R$ 2.994,01 até R$ 3.992,00",
    "H":	"De R$ 3.992,01 até R$ 4.990,00",
    "I":	"De R$ 4.990,01 até R$ 5.988,00",
    "J":	"De R$ 5.988,01 até R$ 6.986,00",
    "K":	"De R$ 6.986,01 até R$ 7.984,00",
    "L":	"De R$ 7.984,01 até R$ 8.982,00",
    "M":	"De R$ 8.982,01 até R$ 9.980,00",
    "N":	"De R$ 9.980,01 até R$ 11.976,00",
    "O":	"De R$ 11.976,01 até R$ 14.970,00",
    "P":	"De R$ 14.970,01 até R$ 19.960,00",
    "Q":	"Mais de R$ 19.960,00"   
})

logger.info("Pega os dados do Enem velhos na tabela Delta...")
enemvelho = DeltaTable.forPath(spark, "s3://datalakehouse-eurico-enem2019/staging-zone/enem")



##como realizar a inserção, com controle de versão quando não há uma chave primaria??
logger.info("Realiza o UPSERT...")
(
    enemvelho.alias("old")
    .merge(dados_filtrados.alias("new"), "old.NU_INSCRICAO = new.NU_INSCRICAO")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

logger.info("Atualizacao completa! \n\n")

logger.info("Gera manifesto symlink...")
enemvelho.generate("symlink_format_manifest")

logger.info("Manifesto gerado.")