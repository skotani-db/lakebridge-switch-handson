# Databricks notebook source
# MAGIC %md
# MAGIC # Lakebridge Switch ハンズオン用ノートブック
# MAGIC 本ノートブックを自身のワークスペースにクローンして実行してください。`Run all`で実行頂いてOKです。

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Lakebridge Switchインストール
# MAGIC PyPiからSwitchのパッケージをインストールします。

# COMMAND ----------

# MAGIC %pip install databricks-switch-plugin --force-reinstall
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lakebridge Switchジョブ作成
# MAGIC 以下のセルを実行することで `lakebridge-switch` という名前のジョブが作成されます。

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from switch.api.installer import SwitchInstaller

ws = WorkspaceClient()
installer = SwitchInstaller(ws)
result = installer.install()

print(f"Switch job created: {result.job_url}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. 作成されたジョブのパラメータを変更して実行
# MAGIC - 上記セルの出力結果にSwitchのジョブURLが表示されるので、そちらのURLをクリック
# MAGIC - 「別の設定で今すぐ実行」をクリック
# MAGIC - 以下のパラメータの値を変更し、ジョブを実行 (複数候補があるものは好きなものを選んでOK)
# MAGIC - input_dir は`<your-email>`と`<builtin_prompt>`を置き換えることを忘れないように注意
# MAGIC
# MAGIC パラメータ | 値
# MAGIC --- | ---
# MAGIC builtin_prompt | `snowflake` / `mysql` / `netezza` / `oracle` / `postgresql` / `redshift` / `teradata` / `tsql`
# MAGIC comment_lang | `Japanese` / `Chinese` / `Korean` / `English`
# MAGIC endpoint_name | `databricks-gpt-oss-120b` / `databricks-gpt-oss-20b`
# MAGIC input_dir | `/Workspace/Users/<your-email>/lakebridge-switch-handson/examples/<builtin_prompt>/input`
# MAGIC output_dir | 自身のホームフォルダ内に任意のフォルダを作成し、そのパスを指定 (特になければ `/Workspace/Users/<your-email>/lakebridge-switch-handson/handson_output`)
# MAGIC result_catalog | 任意のカタログ (特になければ `main` 、事前に作成する必要あり)
# MAGIC result_schema | 上記カタログ内の任意のスキーマ (特になければ `switch_handson_<your-name>` 、事前に作成する必要あり )
# MAGIC
# MAGIC ## 4. ジョブの実行内容の確認
# MAGIC ジョブが完了するまで10分ほど待つ。その間に実行されるノートブックの処理内容を観察する。
# MAGIC
# MAGIC ## 5. ジョブの実行結果の確認
# MAGIC ジョブが完了したら `output_dir` に作成されたノートブックを確認する。
# MAGIC
# MAGIC 以上でハンズオン完了。
