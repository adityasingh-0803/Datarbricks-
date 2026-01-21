# Day 13: Model Comparison & Feature Engineering
# Topics: Multiple models, MLflow comparison, Spark ML Pipelines

import mlflow
import mlflow.sklearn
from sklearn.linear_model import LinearRegression
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split

# -----------------------------------
# 1. Prepare data
# -----------------------------------
df = spark.table("gold.products").toPandas()

X = df[["views", "cart_adds"]] if "cart_adds" in df.columns else df[["views"]]
y = df["purchases"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# -----------------------------------
# 2. Train multiple models & log to MLflow
# -----------------------------------
models = {
    "linear_regression": LinearRegression(),
    "decision_tree": DecisionTreeRegressor(max_depth=5),
    "random_forest": RandomForestRegressor(n_estimators=100, random_state=42)
}

mlflow.set_experiment("Databricks_Model_Comparison")

for name, model in models.items():
    with mlflow.start_run(run_name=name):
        mlflow.log_param("model_type", name)

        model.fit(X_train, y_train)
        score = model.score(X_test, y_test)

        mlflow.log_metric("r2_score", score)
        mlflow.sklearn.log_model(model, artifact_path="model")

        print(f"{name} → R² score: {score:.4f}")

# -----------------------------------
# 3. Spark ML Pipeline
# -----------------------------------
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression as SparkLR

spark_df = spark.table("gold.products")

assembler = VectorAssembler(
    inputCols=["views", "cart_adds"] if "cart_adds" in spark_df.columns else ["views"],
    outputCol="features"
)

lr = SparkLR(featuresCol="features", labelCol="purchases")

pipeline = Pipeline(stages=[assembler, lr])

train_df, test_df = spark_df.randomSplit([0.8, 0.2], seed=42)

pipeline_model = pipeline.fit(train_df)

print("Spark ML pipeline trained successfully.")
