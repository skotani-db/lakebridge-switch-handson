# Databricks notebook source
# MAGIC %md
# MAGIC # snowflake_example2_javascript_procedure
# MAGIC このノートブックは以下のスクリプトから自動的に変換されました。エラーが含まれている可能性があるため、出発点として使用し、必要な修正を行ってください。
# MAGIC
# MAGIC ソーススクリプト: `/Workspace/Users/shotaro.kotani@databricks.com/lakebridge-switch-handson/examples/snowflake/input/snowflake_example2_javascript_procedure.sql`

# COMMAND ----------

# パラメータ用ウィジェットを作成（デフォルトは 1.30）
dbutils.widgets.text("SCHEMA_NAME", "")
dbutils.widgets.text("OUTLIER_MULTIPLIER", "1.30")

# COMMAND ----------

# パラメータ取得
schema_name = dbutils.widgets.get("SCHEMA_NAME").strip()
try:
    outlier_multiplier = float(dbutils.widgets.get("OUTLIER_MULTIPLIER"))
except ValueError:
    raise ValueError("OUTLIER_MULTIPLIER は数値である必要があります。")

# COMMAND ----------

# 初期化
result = 0
error_msg = None
current_date = None
restore_timestamp = None  # ロールバック用に取得しておく

# COMMAND ----------

try:
    # ------------------------------------------------------------
    # 1) トランザクション開始相当の処理（Databricks では try ブロックで代用）
    # ------------------------------------------------------------

    # 2) SystemDateTable からシステム日付を取得
    sql_get_date = f"""
        SELECT SystemDate
        FROM `{schema_name}`.SystemDateTable
        """
    df_date = spark.sql(sql_get_date)
    row = df_date.first()
    if row is None or row["SystemDate"] is None:
        raise RuntimeError("SystemDateTable から日付を取得できませんでした。")
    current_date = row["SystemDate"]  # timestamp または date

    # ------------------------------------------------------------
    # 3) TEMP_OUTLIER_INFO テーブル（Delta テーブル）を作成
    # ------------------------------------------------------------
    spark.sql("""
        CREATE OR REPLACE TABLE TEMP_OUTLIER_INFO (
            LocationId STRING,
            OutlierThreshold DECIMAL(8,2)
        )
        """)

    # ------------------------------------------------------------
    # 4) 99 パーセンタイル閾値を計算して TEMP_OUTLIER_INFO に挿入
    # ------------------------------------------------------------
    sql_insert_thresholds = f"""
        INSERT INTO TEMP_OUTLIER_INFO (LocationId, OutlierThreshold)
        SELECT
            d.LocationId,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY d.MetricValue) OVER (PARTITION BY d.LocationId) AS OutlierThreshold
        FROM `{schema_name}`.HistoricalDataTable d
        WHERE TO_DATE(d.TargetDate) >= DATEADD(YEAR, -1, TO_DATE('{current_date}'))
        """
    spark.sql(sql_insert_thresholds)

    # ------------------------------------------------------------
    # 5) 変更前の ForecastValue を OriginalForecastValue に保存
    #    （MERGE を使用して UPDATE FROM をエミュレート）
    # ------------------------------------------------------------
    sql_merge_save_originals = f"""
        MERGE INTO `{schema_name}`.ForecastTable AS f
        USING TEMP_OUTLIER_INFO AS t
        ON f.LocationId = t.LocationId
           AND TO_DATE(f.ForecastDate) = TO_DATE('{current_date}')
        WHEN MATCHED AND f.ForecastValue > t.OutlierThreshold * {outlier_multiplier}
        THEN UPDATE SET f.OriginalForecastValue = f.ForecastValue
        """
    spark.sql(sql_merge_save_originals)

    # ------------------------------------------------------------
    # 6) 閾値 * multiplier を超える予測値を上限にキャップ
    # ------------------------------------------------------------
    sql_merge_update_outliers = f"""
        MERGE INTO `{schema_name}`.ForecastTable AS f
        USING TEMP_OUTLIER_INFO AS t
        ON f.LocationId = t.LocationId
           AND TO_DATE(f.ForecastDate) = TO_DATE('{current_date}')
        WHEN MATCHED AND f.ForecastValue > t.OutlierThreshold * {outlier_multiplier}
        THEN UPDATE SET f.ForecastValue = t.OutlierThreshold * {outlier_multiplier}
        """
    spark.sql(sql_merge_update_outliers)

    # ------------------------------------------------------------
    # 7) 正常終了：コミット相当（何もしなくても OK）
    # ------------------------------------------------------------

except Exception as e:
    # エラーメッセージ保存
    error_msg = str(e)
    result = 2

    # ------------------------------------------------------------
    # ロールバック処理（Delta テーブルの履歴から復元）
    # ------------------------------------------------------------
    try:
        # ForecastTable の最新バージョンのタイムスタンプを取得
        hist_df = spark.sql(f"DESCRIBE HISTORY `{schema_name}`.ForecastTable LIMIT 1")
        hist_row = hist_df.first()
        if hist_row:
            restore_timestamp = hist_row["timestamp"]
            # テーブルを取得した時点に復元
            spark.sql(f"RESTORE TABLE `{schema_name}`.ForecastTable TO TIMESTAMP AS OF '{restore_timestamp}'")
    except Exception as restore_err:
        # 復元に失敗した場合はログに残すだけ
        print(f"Rollback に失敗しました: {restore_err}")

    # エラーログを別テーブルに記録する例（存在しない場合はコメントアウト）
    # log_sql = f\"\"\"INSERT INTO `{schema_name}`.ErrorLog (ProcedureName, ErrorMessage) VALUES ('DEMO_FORECAST_OUTLIER_CHECK_UPDATE', '{error_msg.replace(\"'\", \"''\")}')\"\"\"
    # try:
    #     spark.sql(log_sql)
    # except Exception:
    #     pass

    # 例外を再送出してノートブックを終了
    raise

finally:
    # ------------------------------------------------------------
    # 後処理：一時テーブルを削除
    # ------------------------------------------------------------
    try:
        spark.sql("DROP TABLE IF EXISTS TEMP_OUTLIER_INFO")
    except Exception as drop_err:
        print(f"TEMP_OUTLIER_INFO の削除に失敗しました: {drop_err}")

# COMMAND ----------

# 結果を文字列で返す（Databricks では print または dbutils.notebook.exit が利用可）
result_str = str(result)
print(f"Procedure 実行結果: {result_str}")
dbutils.notebook.exit(result_str)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 静的構文チェック結果
# MAGIC 静的チェック中に構文エラーは検出されませんでした。
# MAGIC ただし、一部の問題は実行時にのみ検出される可能性があるため、コードを注意深く確認してください。
