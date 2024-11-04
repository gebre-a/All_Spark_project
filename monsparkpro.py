from pyspark.sql import SparkSession

# Initialize SparkSession with MongoDB configuration
spark = SparkSession.builder \
    .appName("Spark MongoDB Integration") \
    .config("spark.mongodb.input.uri", "mongodb://localhost/mongspark.table") \
    .config("spark.mongodb.output.uri", "mongodb://localhost/mongspark.table") \
    .getOrCreate()

# Load data from a CSV file (replace with your file path)
data = spark.read.csv(r'C:\Users\kidan\OneDrive\Desktop\data scince jupyter\Salary_Data.csv', header=True, inferSchema=True)

# Show the initial data
print("Initial Data:")
data.show()

# Process the data (filtering, selecting, transforming)
selected_data = data.select("YearsExperience", "Salary").filter(data['YearsExperience'] > 2)
transformed_data = selected_data.withColumn('new_column', selected_data['YearsExperience'] + 100)

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
result = spark.sql("SELECT column1, AVG(new_column) as avg_value FROM data_view GROUP BY column1")

# Show the result of the query
print("Query Result:")
result.show()

# Stop the SparkSession
spark.stop()
