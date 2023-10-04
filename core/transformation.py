from core import reading
from pyspark.sql.functions import to_timestamp, unix_timestamp, dayofweek, radians, acos, sin, cos
from pyspark.sql.functions import col, isnan, when, count

raw_df = reading.reading_csv()


def check_null(df):
    return df.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df.columns])


# check_null(raw_df).show()


def drop_na(df):
    df = df.na.drop(how='any')
    return df


removed_null = drop_na(raw_df)


# check_null(removed_null).show()


def check_duplicate(df):
    # Group by all columns and count the occurrences of each row
    duplicate_counts = df.groupBy(df.columns).count()

    # Filter for rows with a count greater than 1 to find duplicates
    duplicate_counts = duplicate_counts.filter(col("count") > 1)
    return duplicate_counts


# got no duplicate rows
# check_duplicate(removed_null).show()
# removed_null.printSchema()


# changing datatype of started_at and ended_at to datetime
# 2021-05-02 09:20:46


def to_date_time(df):
    df = df.withColumn("started_at", to_timestamp("started_at", "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("ended_at", to_timestamp("ended_at", "yyyy-MM-dd HH:mm:ss"))
    return df


df_added_date = to_date_time(removed_null)


# adding a column for trip_duration; ended_at - started_at
def add_col_trip_duration(df):
    df = df.withColumn("trip_duration", col("ended_at") - col("started_at"))
    return df


df_added_col = add_col_trip_duration(df_added_date)


# df_added_col.printSchema()


# df_added_col.select("trip_duration").show()


def calculate_timestamp_difference(df, timestamp_col1, timestamp_col2):
    # Calculate the difference in seconds
    df_with_seconds = df.withColumn(
        "timestamp_difference_seconds",
        (unix_timestamp(timestamp_col1) - unix_timestamp(timestamp_col2))
    )

    # Calculate the difference in hours
    df_with_hours = df_with_seconds.withColumn(
        "timestamp_difference_hours",
        (col("timestamp_difference_seconds") / 3600.0)
    )

    return df_with_hours


# Calculate the timestamp difference using the function
timestamp_diff = calculate_timestamp_difference(df_added_col, "ended_at", "started_at")


# Show the result
# timestamp_diff.show()
# timestamp_diff.printSchema()


def get_day_of_week(df):
    df = df.withColumn("day_of_week", dayofweek("started_at"))

    df = df.withColumn('Day',
                       when(col('day_of_week') == 1, 'Sunday')
                       .when(col('day_of_week') == 2, 'Monday')
                       .when(col('day_of_week') == 3, 'Tuesday')
                       .when(col('day_of_week') == 4, 'Wednesday')
                       .when(col('day_of_week') == 5, 'Thursday')
                       .when(col('day_of_week') == 6, 'Friday')
                       .otherwise('Saturday'))

    return df


df_with_day_of_week = get_day_of_week(timestamp_diff)


# df_with_day_of_week.show(5)

# Function to calculate the distance between two points specified by latitude and longitude
def calculate_distance(df, start_lat_col, start_lng_col, end_lat_col, end_lng_col):
    # Convert latitudes and longitudes from degrees to radians
    df = df.withColumn("start_lat_rad", radians(col(start_lat_col)))
    df = df.withColumn("start_lng_rad", radians(col(start_lng_col)))
    df = df.withColumn("end_lat_rad", radians(col(end_lat_col)))
    df = df.withColumn("end_lng_rad", radians(col(end_lng_col)))

    # Calculate the Haversine distance in kilometers
    earth_radius_km = 6371.0  # Earth's radius in kilometers

    df = df.withColumn(
        "distance_km",
        earth_radius_km * acos(
            sin(col("start_lat_rad")) * sin(col("end_lat_rad")) +
            cos(col("start_lat_rad")) * cos(col("end_lat_rad")) *
            cos(col("end_lng_rad") - col("start_lng_rad"))
        )
    )

    # Calculate the distance in meters
    earth_radius_meters = 6371000.0  # Earth's radius in meters

    df = df.withColumn(
        "distance_meters",
        earth_radius_meters * acos(
            sin(col("start_lat_rad")) * sin(col("end_lat_rad")) +
            cos(col("start_lat_rad")) * cos(col("end_lat_rad")) *
            cos(col("end_lng_rad") - col("start_lng_rad"))
        )
    )

    return df


# Calculate the distance using the function
cleaned_df = calculate_distance(df_with_day_of_week, "start_lat", "start_lng", "end_lat", "end_lng")
