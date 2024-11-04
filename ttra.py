from pyspark.sql import SparkSession

# Initialize SparkSession with MongoDB configuration and add the MongoDB connector package
spark = SparkSession.builder \
    .appName("Spark MongoDB Integration") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/mongspark.table") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/mongspark.table") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.1.1") \
    .getOrCreate()

# Load data from a CSV file (replace with your file path)
data = spark.read.csv(r'C:\Users\kidan\OneDrive\Desktop\data scince jupyter\Salary_Data.csv', header=True, inferSchema=True)

# Show the initial data
print("Initial Data:")
data.show()

# Process the data (filtering, selecting, transforming)
selected_data = data.select("YearsExperience", "Salary").filter(data['YearsExperience'] > 2)

# Adding a new column by transforming the existing column
transformed_data = selected_data.withColumn('AdjustedSalary', selected_data['Salary'] + 5000)

# Show transformed data
print("Transformed Data:")
transformed_data.show()

# Write transformed data to MongoDB
transformed_data.write \
    .format("mongo") \
    .mode("append") \
    .option("uri", "mongodb://127.0.0.1/mongspark.table") \
    .save()

# Perform a query using Spark SQL
transformed_data.createOrReplaceTempView("data_view")
result = spark.sql("SELECT YearsExperience, AVG(AdjustedSalary) as avg_salary FROM data_view GROUP BY YearsExperience")

# Show the result of the query
print("Query Result:")
result.show()

# Stop the SparkSession
spark.stop()
