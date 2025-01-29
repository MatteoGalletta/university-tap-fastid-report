# %%
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, from_json, udf, pandas_udf, PandasUDFType
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.conf import SparkConf
from datetime import datetime, timezone, timedelta
from pyspark.sql.types import StringType, IntegerType, TimestampType, StructType, StructField, DoubleType, ArrayType
import os
import psycopg2

# %%
DEBUG_MODE = os.environ.get('APP_DEBUG_MODE', 'True').lower() in ('1', 't', 'true') # if True, doesn't use the cluster, doesn't save in db and collects and the end

# %%
db_conn_string = os.environ.get('APP_CONN_STRING', "postgresql://postgres/fastid-report?user=postgres&password=tappino2025")
db_jdbc_url = os.environ.get('APP_JDBC_URL', "jdbc:postgresql://postgres:5432/fastid-report")
db_jdbc_properties = {
	"user": os.environ.get('APP_JDBC_USERNAME', "postgres"),
	"password": os.environ.get('APP_JDBC_PASSWORD', "tappino2025"),
	"driver": os.environ.get('APP_JDBC_DRIVER', "org.postgresql.Driver")
}

# %%
		#.master("spark://spark:7077") \
spark = SparkSession.builder \
		.appName("FastIDReport") \
		.getOrCreate()
		#.config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.postgresql:postgresql:42.7.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.16.2") \
		#.config("spark.sql.shuffle.partitions", 10) \
		#.master("local[*]") \
		#.config("spark.jars.ivy", "/tmp") \
		#.config("spark.local.dir", "/tmp") \
#        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.postgresql:postgresql:42.7.4,org.elasticsearch:elasticsearch-spark-30_2.12:8.17.1') \

		#.config("spark.sql.streaming.checkpointLocation", "/tmp") \
spark.sparkContext.setLogLevel("INFO")
#spark.sparkContext.setCheckpointDir("/home/jovyan/work/spark-checkpoints")

# %%
TRANSACTION_SCHEMA = StructType([
	StructField("id", IntegerType(), True),
	StructField("Date", TimestampType(), True),
	StructField("InstanceCode", StringType(), True),
	StructField("InstanceType", StringType(), True),
	StructField("User_Id", IntegerType(), True),
	StructField("User_Name", StringType(), True),
	StructField("Type", StringType(), True),
	StructField("AttrSet", StringType(), True),
	StructField("Response_Assertion_AuthnContextClassRef", StringType(), True),
	StructField("uniqueIdpIdentity", StringType(), True),
])

# %%
df_transactions_raw = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "kafkaServer:9092") \
	.option("subscribe", "transactions") \
	.option("startingOffsets", "earliest") \
	.load()
	# .option("startingOffsets", "{\"transactions\":{\"0\":589000}}") \

'''

# %%
_df_transactions = df_transactions_raw \
		.selectExpr("CAST(value AS STRING)") \
		.select(from_json(col("value"), TRANSACTION_SCHEMA).alias("data")) \
		.select("data.*")

# %%
df_transactions = _df_transactions \
	.repartition("User_Name") \
	.withWatermark("Date", "10 seconds") \


# %% [markdown]
# # Clienti

# %%
df_clienti = df_transactions \
	.withColumn("Cliente", F.col("User_Name")) \
	.select("Cliente") \
	.distinct()

#df_clienti.writeStream.outputMode("update").foreachBatch(lambda df, _: save_clienti(df)).start()

# %%
def append_clienti(df):
	c = df.count()
	print(f"[TAP]: Writing {c} consumers...")
	if c > 0:
		df.write.jdbc(url=db_jdbc_url, table='"Clienti"', mode="append", properties=db_jdbc_properties)
query = df_clienti.writeStream \
	.outputMode("append") \
	.option("checkpointLocation", "/tmp/checkpoint-clienti") \
	.foreachBatch(lambda df, _: append_clienti(df)) \
	.start()

if DEBUG_MODE:
	try:
		query.awaitTermination()
	except KeyboardInterrupt:
		query.stop()

# %% [markdown]
# # IdP

# %%
query = df_transactions \
	.withColumn("prefix", F.substring(F.col("uniqueIdpIdentity"), 1, 4)) \
	.filter(~F.col("prefix").isin("UII_", "TENV")) \
	.select("User_Name", "prefix") \
	.groupBy("User_Name", "prefix") \
	.agg(F.count("*").alias("Count")) \
	.select("User_name", "prefix", "Count") \
	.withColumn("id", F.concat(F.col("User_Name"), F.lit("_"), F.col("prefix"))) \
	.withColumn("created_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
	.writeStream \
	.outputMode("update") \
	.option("checkpointLocation", "/tmp/checkpoint-idps") \
	.option("es.nodes", "elasticsearch") \
	.option("es.mapping.id", "id") \
	.option("es.port", "9200") \
	.format("es") \
	.start("count-by-idp")

if DEBUG_MODE:
	try:
		query.awaitTermination()
	except KeyboardInterrupt:
		query.stop()

# %% [markdown]
# # Count by Type

# %%
query = df_transactions \
	.groupBy("User_Name", "Type") \
	.agg(F.count("*").alias("Count")) \
	.select("User_name", "Type", "Count") \
	.withColumn("id", F.concat(F.col("User_Name"), F.lit("_"), F.col("Type"))) \
	.withColumn("created_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
	.writeStream \
	.outputMode("update") \
	.option("checkpointLocation", "/tmp/checkpoint-count-by-type") \
	.option("es.nodes", "elasticsearch") \
	.option("es.mapping.id", "id") \
	.option("es.port", "9200") \
	.format("es") \
	.start("count-by-type")

if DEBUG_MODE:
	try:
		query.awaitTermination()
	except KeyboardInterrupt:
		query.stop()

# %% [markdown]
# # Count by InstanceType

# %%
query = df_transactions \
	.groupBy("User_Name", "InstanceType") \
	.agg(F.count("*").alias("Count")) \
	.select("User_name", "InstanceType", "Count") \
	.withColumn("id", F.concat(F.col("User_Name"), F.lit("_"), F.col("InstanceType"))) \
	.withColumn("created_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
	.writeStream \
	.outputMode("update") \
	.option("checkpointLocation", "/tmp/checkpoint-count-by-instance-type") \
	.option("es.nodes", "elasticsearch") \
	.option("es.mapping.id", "id") \
	.option("es.port", "9200") \
	.format("es") \
	.start("count-by-instance-type")



# %% [markdown]
# # Count by AttrSet

# %%
query = df_transactions \
	.groupBy("User_Name", "AttrSet") \
	.agg(F.count("*").alias("Count")) \
	.select("User_name", "AttrSet", "Count") \
	.withColumn("id", F.concat(F.col("User_Name"), F.lit("_"), F.col("AttrSet"))) \
	.withColumn("created_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
	.writeStream \
	.outputMode("update") \
	.option("checkpointLocation", "/tmp/checkpoint-count-by-attr-set") \
	.option("es.nodes", "elasticsearch") \
	.option("es.mapping.id", "id") \
	.option("es.port", "9200") \
	.format("es") \
	.start("count-by-attr-set")

if DEBUG_MODE:
	try:
		query.awaitTermination()
	except KeyboardInterrupt:
		query.stop()

# %% [markdown]
# # Count by Response_Assertion_AuthnContextClassRef

# %%
query = df_transactions \
	.groupBy("User_Name", "Response_Assertion_AuthnContextClassRef") \
	.agg(F.count("*").alias("Count")) \
	.select("User_name", "Response_Assertion_AuthnContextClassRef", "Count") \
	.withColumn("id", F.concat(F.col("User_Name"), F.lit("_"), F.col("Response_Assertion_AuthnContextClassRef"))) \
	.withColumn("created_at", F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX")) \
	.writeStream \
	.outputMode("update") \
	.option("checkpointLocation", "/tmp/checkpoint-count-by-class-ref") \
	.option("es.nodes", "elasticsearch") \
	.option("es.mapping.id", "id") \
	.option("es.port", "9200") \
	.format("es") \
	.start("count-by-class-ref")

if DEBUG_MODE:
	try:
		query.awaitTermination()
	except KeyboardInterrupt:
		query.stop()

# %% [markdown]
# # Interval Count

# %%
df_interval_count = df_transactions \
	.groupBy("User_Name", F.window("Date", "15 minute")) \
	.agg(F.count("*").alias("TransactionsCount")) \
	.withColumn("Date", F.col("window.start")) \
	.select("Date", "User_Name", "TransactionsCount")

# df_interval_count.printSchema()

# %%
def append_interval_count(df):
	c = df.count()
	print(f"[TAP]: Writing {c} interval counts...")
	if c > 0:
		df.write.jdbc(url=db_jdbc_url, table='"TransactionsCount"', mode="append", properties=db_jdbc_properties)

query = df_interval_count.writeStream \
	.outputMode("append") \
	.option("checkpointLocation", "/tmp/checkpoint-transactions-count-pg") \
	.foreachBatch(lambda df, _: append_interval_count(df)) \
	.start()

if DEBUG_MODE:
	try:
		query.awaitTermination()
	except KeyboardInterrupt:
		query.stop()



# %%
query = df_interval_count \
    .withColumn("key", F.concat(F.col("User_Name"), F.lit("_"), F.col("Date"))) \
    .select(F.col("key"), F.to_json(F.struct("Date", "User_Name", "TransactionsCount")).alias("value")) \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafkaServer:9092") \
    .option("checkpointLocation", "/tmp/checkpoint-transactions-count") \
    .option("topic", "customer-transactions-count") \
    .start()

if DEBUG_MODE:
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        query.stop()


# print("Awaiting termination...")
#spark.streams.awaitAnyTermination()
# print("Terminated.")

'''

# %% [markdown]
# # Online Training

# %%
df_transactions_count_raw = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", "kafkaServer:9092") \
	.option("subscribe", "customer-transactions-count") \
	.option("startingOffsets", "earliest") \
	.load()
	#.option("startingOffsets", "{\"customer-transactions-count\":{\"0\":589000}}") \
	#.option("maxOffsetsPerTrigger", 1) \


# %%
df_transactions_count = df_transactions_count_raw \
	.selectExpr("CAST(value AS STRING)") \
	.select(from_json(col("value"), "Date TIMESTAMP, User_Name STRING, TransactionsCount INT").alias("data")) \
	.select("data.*") \
	.filter(F.col("Date") > "2024-01-01")
	#.filter(F.col("User_Name") == "infallible_nicotra")

# %%
from pyspark.sql.streaming.state import GroupStateTimeout
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures, StandardScaler
from sklearn.ensemble import RandomForestRegressor
from sklearn.pipeline import Pipeline
import numpy as np
import pandas as pd

def update_user_state(key, data, state):

	def train_model_and_predict(transactions_count, toPredictDates):

		def extract_features(dates):
			return pd.DataFrame({
				"timestamp": [int(datetime.timestamp(x)) for x in dates],
				"hour": [x.hour for x in dates],
				"weekday": [x.weekday() for x in dates],
				"is_weekend": [1 if x.weekday() >= 5 else 0 for x in dates]
			})

		X = extract_features(transactions_count["Date"])
		y = transactions_count["TransactionsCount"].values

		model = Pipeline([
			("lr", LinearRegression()),
			#("poly", PolynomialFeatures(degree=2)),
			#("scaler", StandardScaler()),  # Add scaling
			#("rf", RandomForestRegressor(n_estimators=100, random_state=42))
		])
		model.fit(X, y)
		
		X_new = extract_features(toPredictDates)
		Y_new = model.predict(X_new)
		
		#print({
		#	"[TAP]_User_Name": key[0],
		#	"X": X.to_dict(),
		#	"y": y.tolist(),
		#	"X_new": X_new.to_dict(),
		#	"Y_new": Y_new.tolist()
		#})
		
		return [max(0, int(round(x))) for x in Y_new]

	if state.hasTimedOut: # never
		state.remove()
		
	else:
		for batch in data:

			if state.exists:
				(past_transactions, last_date) = state.get
				#(f"[TAP] got state of {key[0]}:", past_transactions, last_date)
				past_transactions_list = [{"Date": k, "TransactionsCount": v} for k, v in past_transactions.items()]
				df_past_transactions = pd.DataFrame(past_transactions_list)
			else:
				df_past_transactions = pd.DataFrame([], columns=["Date", "TransactionsCount"])
				#print("brand new state")                
				last_date = None

			df_batch = pd.DataFrame(batch)
			min_date = (last_date + timedelta(minutes=5)) if last_date is not None else df_batch["Date"].min()
			max_date = df_batch["Date"].max()
			if last_date is not None and last_date > min_date:
				print("[TAP]: WARNING: last_date > min_date [", last_date, "|", min_date, "]")

			all_dates = pd.date_range(start=min_date, end=max_date, freq='5T')
			df_all_dates = pd.DataFrame(all_dates, columns=["Date"])

			df_batch = pd.merge(df_all_dates, df_batch, on="Date", how="left").fillna(0)
			df_batch["User_Name"] = key[0]
			
			start_time = datetime.now()
			print(f"[TAP] Starting processing of user {key[0]} and batch of {len(df_batch)} (original length of {len(batch)}) transactions counts ranging from {min_date} to {max_date}...")

			df = pd.DataFrame([], columns=["User_Name", "Date", "PredictedTransactionsCount", "IsAfterRetrain"])
			for index, row in df_batch.iterrows():                
				
				if len(df_past_transactions) >= 2:
					predicted_counts = train_model_and_predict(df_past_transactions,
						[row["Date"]] if last_date is None else [row["Date"], last_date])
					
					records = [{
						"User_Name": row["User_Name"],
						"Date": row["Date"],
						"PredictedTransactionsCount": predicted_counts[0],
						"IsAfterRetrain": False
					}]
					if last_date is not None:
						records.append({
							"User_Name": row["User_Name"],
							"Date": last_date,
							"PredictedTransactionsCount": predicted_counts[1],
							"IsAfterRetrain": True
						})

					df = pd.concat([df, pd.DataFrame.from_records(records)])

				#print(f"[TAP]\tSingle Transactions Count for user {key[0]} at {row['Date']} processed...")

				df_past_transactions = pd.concat([df_past_transactions, pd.DataFrame.from_records([{
					"Date": row["Date"],
					"TransactionsCount": row["TransactionsCount"]
				}])])

				df_past_transactions = df_past_transactions[df_past_transactions["Date"] > row["Date"] - timedelta(days=30)]

				last_date = row["Date"] # KISS
			

			print(f"[TAP] Saving processing of user {key[0]} and batch of {len(df_batch)} transactions counts ranging from {min_date} to {max_date}...")
			
			# we save all transactions in state
			x = pd.Series(df_past_transactions['TransactionsCount'].values, index=df_past_transactions['Date']).to_dict()
			state.update((x, last_date))
		
			elapsed_time = datetime.now() - start_time
			print(f"[TAP] Ending processing (took: { elapsed_time }) of user {key[0]} and batch of {len(df_batch)} transactions counts ranging from {min_date} to {max_date}...")
			#print("df", df)

			yield df

def append_predicted_transactions_count(df):
	c = df.count()
	print(f"[TAP]: Writing {c} predicted transactions counts...")
	if c > 0:
		df.write.jdbc(url=db_jdbc_url, table='"PredictedTransactionsCount"', mode="append", properties=db_jdbc_properties)


query = df_transactions_count \
	.withWatermark("Date", "10 seconds") \
	.groupBy("User_Name") \
	.applyInPandasWithState(
		update_user_state,
		"User_Name STRING, Date TIMESTAMP, PredictedTransactionsCount INT, IsAfterRetrain BOOLEAN",
		"TransactionsCount MAP<TIMESTAMP, INT>, LastDate TIMESTAMP",
		"update",
		GroupStateTimeout.NoTimeout
	) \
	.withColumn("created_at", F.current_timestamp()) \
	.writeStream \
	.option("checkpointLocation", "/tmp/checkpoint-predicted-transactions-count") \
	.outputMode("update") \
	.foreachBatch(lambda df, _: append_predicted_transactions_count(df)) \
	.start()

if DEBUG_MODE:
	try:
		query.awaitTermination()
	except KeyboardInterrupt:
		query.stop()

# %%
if not DEBUG_MODE:
	spark.streams.awaitAnyTermination()


