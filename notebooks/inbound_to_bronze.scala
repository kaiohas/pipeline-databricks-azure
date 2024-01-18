// Databricks notebook source
// MAGIC %md
// MAGIC ###Conferindo se dados foram montados e se temos acesso a pasta inbound

// COMMAND ----------

display(dbutils.fs.ls("/mnt/dados/inbound"))

// COMMAND ----------

// MAGIC %md
// MAGIC #Lendo os dados na camanda de inbound

// COMMAND ----------

val path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
val dados = spark.read.json(path)

// COMMAND ----------

display(dados)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Removendo Colunas
// MAGIC

// COMMAND ----------

val dados_anuncio = dados.drop("imagens","usuario")
display(dados_anuncio)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Criando uma coluna de identificação

// COMMAND ----------

import org.apache.spark.sql.functions.col

// COMMAND ----------

val df_bronze = dados_anuncio.withColumn("id",col("anuncio.id"))
display(df_bronze)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Salvando na camada Bronze
// MAGIC

// COMMAND ----------




// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode(SaveMode.Overwrite).save(path)

// COMMAND ----------


