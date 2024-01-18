// Databricks notebook source


// COMMAND ----------

display(dbutils.fs.ls("/mnt/dados/bronze"))

// COMMAND ----------

val path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
val df = spark.read.format("delta").load(path)
display(df)

// COMMAND ----------

// MAGIC %md
// MAGIC ##Transformando os campos Json em colunas

// COMMAND ----------

display(df.select("anuncio.*"))

// COMMAND ----------

val dados_detalhados = df.select("anuncio.*","anuncio.endereco.*")


// COMMAND ----------

display(dados_detalhados)

// COMMAND ----------

val df_silver = dados_detalhados.drop("caracteristicas","endereco")

// COMMAND ----------

display(df_silver)

// COMMAND ----------

val path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/mnt/dados/silver/dataset_imoveis"))

// COMMAND ----------

val columnNmaes = df_silver.columns

// COMMAND ----------

val columnNames: Array[String] = df_silver.columns
columnNames

// COMMAND ----------


