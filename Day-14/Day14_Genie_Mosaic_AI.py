# Day 14: AI-Powered Analytics with Genie & Mosaic AI
# Topics: Natural language → SQL, NLP, MLflow logging

import mlflow
from pyspark.sql import functions as F

# -----------------------------------
# 1. Example Analytics Queries (Genie style)
# -----------------------------------

products = spark.table("gold.products")

# Total revenue by category
products.groupBy("category_code") \
    .agg(F.sum("revenue").alias("total_revenue")) \
    .orderBy(F.desc("total_revenue")) \
    .show()

# Top products by conversion rate
products.orderBy(F.desc("conversion_rate")) \
    .select("product_name", "conversion_rate") \
    .show(5)

# Daily purchase trend
spark.table("silver.events") \
    .filter("event_type = 'purchase'") \
    .groupBy("event_date") \
    .count() \
    .orderBy("event_date") \
    .show()

# Users who viewed but never purchased
views = spark.table("silver.events").filter("event_type='view'")
purchases = spark.table("silver.events").filter("event_type='purchase'")

non_buyers = views.join(
    purchases,
    on="user_id",
    how="left_anti"
)

print("Users who never purchased:")
non_buyers.select("user_id").distinct().show()


# -----------------------------------
# 2. Mosaic AI – NLP Sentiment Analysis
# -----------------------------------
from transformers import pipeline

classifier = pipeline("sentiment-analysis")

reviews = [
    "This product is amazing!",
    "Terrible quality, waste of money",
    "Good value for price",
    "Not satisfied with the delivery"
]

results = classifier(reviews)

print("Sentiment Results:", results)


# -----------------------------------
# 3. Log with MLflow
# -----------------------------------
with mlflow.start_run(run_name="mosaic_sentiment_analysis"):

    mlflow.log_param("model_name", "distilbert-sentiment")

    positive = sum(1 for r in results if r["label"] == "POSITIVE")
    accuracy_proxy = positive / len(results)

    mlflow.log_metric("positive_ratio", accuracy_proxy)

    print("MLflow run logged successfully.")
