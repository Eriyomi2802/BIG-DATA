from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Print spark session object
print(f"Spark session object: {spark}")

# Load the text file
text_file = spark.read.text('replace/with_path_to_your/.txt')

# Split the lines into words
words = text_file.rdd.flatMap(lambda line: line.value.split())

# Count the occurrences of each word
word_counts = words.countByValue()

# Print the word counts_
for word, count in word_counts.items():
    print(f"{word}: {count}")

# Stop the SparkSession
spark.stop()