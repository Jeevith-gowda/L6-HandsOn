# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

# Create outputs directory
os.makedirs("outputs", exist_ok=True)

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
print("Loading datasets...")
listening_logs = spark.read.csv("listening_logs.csv", header=True, inferSchema=True)
songs_metadata = spark.read.csv("songs_metadata.csv", header=True, inferSchema=True)

# Display basic info about datasets
print("Listening Logs Schema:")
listening_logs.printSchema()
print(f"Listening Logs Count: {listening_logs.count()}")

print("\nSongs Metadata Schema:")
songs_metadata.printSchema()
print(f"Songs Metadata Count: {songs_metadata.count()}")

# Show sample data
print("\nSample Listening Logs:")
listening_logs.show(5)

print("\nSample Songs Metadata:")
songs_metadata.show(5)

# Task 1: User Favorite Genres
print("\n=== TASK 1: USER FAVORITE GENRES ===")

# Join listening logs with metadata to get genre information
user_genre_listening = listening_logs.join(songs_metadata, "song_id") \
    .select("user_id", "genre", "duration_sec")

# Calculate total listening time per user per genre
user_genre_totals = user_genre_listening \
    .groupBy("user_id", "genre") \
    .agg(sum("duration_sec").alias("total_duration"))

# Find each user's favorite genre (genre with most listening time)
window_spec = Window.partitionBy("user_id").orderBy(desc("total_duration"))
user_favorite_genres = user_genre_totals \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("user_id", "genre", "total_duration") \
    .orderBy("user_id")

print("Each user's favorite genre:")
user_favorite_genres.show()

# Save to CSV
user_favorite_genres.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/task1_user_favorite_genres")
print("✓ Saved Task 1: User Favorite Genres to outputs/task1_user_favorite_genres/")

# Task 2: Average Listen Time
print("\n=== TASK 2: AVERAGE LISTEN TIME ===")

# Overall average listen time
overall_avg = listening_logs.agg(avg("duration_sec").alias("avg_duration")).collect()[0]["avg_duration"]
print(f"Overall Average Listen Time: {overall_avg:.2f} seconds")

# Average listen time by genre (main analysis for this task)
genre_avg = listening_logs.join(songs_metadata, "song_id") \
    .groupBy("genre") \
    .agg(avg("duration_sec").alias("avg_duration")) \
    .orderBy(desc("avg_duration"))

print("Average listen time by genre:")
genre_avg.show()

# Save Task 2 results
genre_avg.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/task2_average_listen_time")
print("✓ Saved Task 2: Average Listen Time to outputs/task2_average_listen_time/")

# Task 3: Genre Loyalty Scores
print("\n=== TASK 3: GENRE LOYALTY SCORES ===")

# Calculate total listening time per user
user_total_time = listening_logs \
    .groupBy("user_id") \
    .agg(sum("duration_sec").alias("total_user_time"))

# Calculate genre loyalty score (% of time spent in favorite genre)
user_loyalty = user_favorite_genres \
    .join(user_total_time, "user_id") \
    .withColumn("loyalty_score", 
                round((col("total_duration") / col("total_user_time")) * 100, 2)) \
    .select("user_id", "genre", "loyalty_score") \
    .orderBy(desc("loyalty_score"))

print("Genre loyalty scores (% of time in favorite genre):")
user_loyalty.show()

# Save loyalty scores
user_loyalty.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/task3_genre_loyalty_scores")
print("✓ Saved Task 3: Genre Loyalty Scores to outputs/task3_genre_loyalty_scores/")

# Summary stats on loyalty
loyalty_stats = user_loyalty.agg(
    avg("loyalty_score").alias("avg_loyalty"),
    min("loyalty_score").alias("min_loyalty"),
    max("loyalty_score").alias("max_loyalty")
).collect()[0]

print(f"Loyalty Score Stats - Avg: {loyalty_stats['avg_loyalty']:.2f}%, "
      f"Min: {loyalty_stats['min_loyalty']:.2f}%, Max: {loyalty_stats['max_loyalty']:.2f}%")

# Task 4: Identify users who listen between 12 AM and 5 AM
print("\n=== TASK 4: LATE NIGHT LISTENERS (12 AM - 5 AM) ===")

# Extract hour from timestamp and filter for late night hours
late_night_logs = listening_logs \
    .withColumn("hour", hour(col("timestamp"))) \
    .filter((col("hour") >= 0) & (col("hour") < 5))

# Create comprehensive late night analysis
late_night_analysis = late_night_logs \
    .join(songs_metadata, "song_id") \
    .groupBy("user_id") \
    .agg(count("*").alias("late_night_sessions"),
         sum("duration_sec").alias("total_late_night_duration"),
         count_distinct("genre").alias("genres_explored")) \
    .orderBy("user_id")

print("Late night listeners analysis:")
late_night_analysis.show()

# Save Task 4 results
late_night_analysis.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/task4_late_night_listeners")
print("✓ Saved Task 4: Late Night Listeners to outputs/task4_late_night_listeners/")

print("\n=== ANALYSIS COMPLETE ===")
print("4 output files saved to outputs/ folder:")
print("- task1_user_favorite_genres/")
print("- task2_average_listen_time/") 
print("- task3_genre_loyalty_scores/")
print("- task4_late_night_listeners/")
spark.stop()