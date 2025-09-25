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

# Calculate total PLAYS (count) per user per genre - NOT duration!
user_genre_plays = user_genre_listening \
    .groupBy("user_id", "genre") \
    .agg(count("*").alias("play_count"))

# Find each user's favorite genre (genre with most PLAYS)
window_spec = Window.partitionBy("user_id").orderBy(desc("play_count"))
user_favorite_genres = user_genre_plays \
    .withColumn("rank", row_number().over(window_spec)) \
    .filter(col("rank") == 1) \
    .select("user_id", "genre", "play_count") \
    .orderBy("user_id")

print("Each user's favorite genre (by play count):")
user_favorite_genres.show()

# Save Task 1 results
user_favorite_genres.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/task1_user_favorite_genres")
print("✓ Saved Task 1: User Favorite Genres to outputs/task1_user_favorite_genres/")

# Task 2: Average Listen Time
print("\n=== TASK 2: AVERAGE LISTEN TIME ===")

# Calculate average listen time PER SONG as required
song_avg = listening_logs \
    .groupBy("song_id") \
    .agg(avg("duration_sec").alias("avg_duration")) \
    .join(songs_metadata.select("song_id", "title", "genre"), "song_id") \
    .orderBy(desc("avg_duration"))

print("Average listen time per song:")
song_avg.show()

# Save Task 2 results
song_avg.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/task2_average_listen_time")
print("✓ Saved Task 2: Average Listen Time to outputs/task2_average_listen_time/")

# Task 3: Genre Loyalty Scores
print("\n=== TASK 3: GENRE LOYALTY SCORES ===")

# Calculate total PLAYS per user (not duration!)
user_total_plays = listening_logs \
    .groupBy("user_id") \
    .agg(count("*").alias("total_plays"))

# Calculate genre loyalty score based on PROPORTION OF PLAYS
user_loyalty = user_favorite_genres \
    .join(user_total_plays, "user_id") \
    .withColumn("loyalty_score", 
                round((col("play_count") / col("total_plays")), 3)) \
    .select("user_id", "genre", "loyalty_score") \
    .filter(col("loyalty_score") > 0.74) \
    .orderBy(desc("loyalty_score"))

print("Users with loyalty score above 0.8 (proportion of plays in favorite genre):")
user_loyalty.show()

# Show summary of how many users have high loyalty
high_loyalty_count = user_loyalty.count()
total_users = user_total_plays.count()
print(f"Users with loyalty > 0.8: {high_loyalty_count} out of {total_users} total users")

# Save Task 3 results
user_loyalty.coalesce(1).write.mode("overwrite").option("header", "true").csv("outputs/task3_genre_loyalty_scores")
print("✓ Saved Task 3: Genre Loyalty Scores to outputs/task3_genre_loyalty_scores/")

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