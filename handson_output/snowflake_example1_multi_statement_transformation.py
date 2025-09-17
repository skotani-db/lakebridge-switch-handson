# Databricks notebook source
# MAGIC %md
# MAGIC # snowflake_example1_multi_statement_transformation
# MAGIC このノートブックは以下のスクリプトから自動的に変換されました。エラーが含まれている可能性があるため、出発点として使用し、必要な修正を行ってください。
# MAGIC
# MAGIC ソーススクリプト: `/Workspace/Users/shotaro.kotani@databricks.com/lakebridge-switch-handson/examples/snowflake/input/snowflake_example1_multi_statement_transformation.sql`

# COMMAND ----------

# 必要なインポート（Databricks ではデフォルトで Spark が利用可能）
# ここでは追加のインポートは不要です

# ------------------------------------------------------------
# Snowflake のセッションタイムゾーン設定は Databricks では不要です
# → 以下のステートメントはコメントアウトして説明のみ残します
# spark.sql("SET TIMEZONE = 'America/Los_Angeles'")  # Databricks ではタイムゾーンはクラスター設定に依存

# ------------------------------------------------------------
# 顧客テーブル (Delta テーブル) の作成
spark.sql("""
CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID BIGINT,
    FULL_NAME STRING,
    STATUS STRING,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
""")

# COMMAND ----------

# ------------------------------------------------------------
# 顧客テーブルへのデータ挿入
spark.sql("""
INSERT INTO CUSTOMERS (CUSTOMER_ID, FULL_NAME, STATUS) VALUES
  (1, 'Alice Smith', 'ACTIVE'),
  (2, 'Bob Jones', 'INACTIVE'),
  (3, 'Charlie Brown', 'ACTIVE')
""")

# COMMAND ----------

# ------------------------------------------------------------
# 一時的な住所テーブル (Delta テーブル) の作成
# Snowflake の TEMPORARY TABLE は Databricks では同等の概念が無いので、Delta テーブルとして作成し、後で削除します
spark.sql("""
CREATE OR REPLACE TABLE TEMP_ADDRESSES (
    ADDRESS_ID BIGINT,
    CUSTOMER_ID BIGINT,
    ADDRESS_LINE STRING
)
""")

# COMMAND ----------

# ------------------------------------------------------------
# 住所テーブルへのデータ挿入
spark.sql("""
INSERT INTO TEMP_ADDRESSES (ADDRESS_ID, CUSTOMER_ID, ADDRESS_LINE) VALUES
  (100, 1, '123 Maple Street'),
  (101, 2, '456 Oak Avenue'),
  (102, 3, '789 Pine Road')
""")

# COMMAND ----------

# ------------------------------------------------------------
# UPDATE: TEMP_ADDRESSES の住所に 'Pine' が含まれる顧客のステータスを 'PENDING' に変更
# Databricks では UPDATE ... FROM がサポートされていないため、MERGE を使用します
spark.sql("""
MERGE INTO CUSTOMERS AS tgt
USING (
    SELECT DISTINCT CUSTOMER_ID
    FROM TEMP_ADDRESSES
    WHERE ADDRESS_LINE LIKE '%Pine%'
) AS src
ON tgt.CUSTOMER_ID = src.CUSTOMER_ID
WHEN MATCHED THEN UPDATE SET STATUS = 'PENDING'
""")

# COMMAND ----------

# ------------------------------------------------------------
# DELETE: INACTIVE な顧客に紐づく住所レコードを削除
# Databricks では DELETE ... USING がサポートされていないため、サブクエリで条件を指定します
spark.sql("""
DELETE FROM TEMP_ADDRESSES
WHERE CUSTOMER_ID IN (
    SELECT CUSTOMER_ID
    FROM CUSTOMERS
    WHERE STATUS = 'INACTIVE'
)
""")

# COMMAND ----------

# ------------------------------------------------------------
# 結果の表示: 顧客テーブル
df_customers = spark.sql("SELECT * FROM CUSTOMERS")
display(df_customers)

# COMMAND ----------

# 結果の表示: 住所テーブル
df_addresses = spark.sql("SELECT * FROM TEMP_ADDRESSES")
display(df_addresses)

# COMMAND ----------

# ------------------------------------------------------------
# 後処理: 作成したテーブルを削除（テスト用スクリプトなのでクリーンアップ）
spark.sql("DROP TABLE IF EXISTS CUSTOMERS")
spark.sql("DROP TABLE IF EXISTS TEMP_ADDRESSES")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 静的構文チェック結果
# MAGIC 静的チェック中に構文エラーは検出されませんでした。
# MAGIC ただし、一部の問題は実行時にのみ検出される可能性があるため、コードを注意深く確認してください。
