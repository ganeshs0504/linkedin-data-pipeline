import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, dayofweek, date_format

spark = SparkSession.builder \
        .appName("linkedin-data-transformations") \
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-bucket-gcs-linkedin-pipeline')

skills_data_path = "gs://gcs-linkedin-data-bucket/processed_data/skills.parquet"
postings_data_path = "gs://gcs-linkedin-data-bucket/processed_data/jobs.parquet"

skills_df = spark.read.parquet(skills_data_path)
jobs_df = spark.read.parquet(postings_data_path)

skills_as_list_df = skills_df.withColumn("skills_list", split(col("skills_list"), ","))
skills_exploded = skills_as_list_df.withColumn("skills_list", explode(col("skills_list")))
skills_dim = skills_exploded.select("job_link", "skills_list")

location_dim = jobs_df.select("default_job_location", "city", "country").distinct()
location_dim = location_dim.withColumn("location_id", monotonically_increasing_id())
postings_df = jobs_df.join(location_dim, on=["default_job_location", "city", "country"], how="left")

date_dim = postings_df.select("first_seen").distinct() \
            .withColumn("date_id", monotonically_increasing_id()) \
            .withColumn("year", year(col("first_seen"))) \
            .withColumn("month", month(col("first_seen"))) \
            .withColumn("day", dayofmonth(col("first_seen"))) \
            .withColumn("day_of_week", dayofweek(col("first_seen"))) \
            .withColumn("month_name", date_format(col("first_seen"), "MMMM"))

postings_df = postings_df.join(date_dim, postings_df.first_seen == date_dim.first_seen, "left") \
                .select(postings_df["*"], date_dim["date_id"])

fact_table = postings_df.select([
    "job_link",
    "job_title",
    "company",
    "search_title",
    "location_id",
    "date_id"
])


fact_table.write.format("bigquery") \
    .option("table", "linkedin_bq_dataset.jobs_fact_table") \
    .mode("overwrite") \
    .save()

skills_dim.write.format("bigquery") \
    .option("table", "linkedin_bq_dataset.skills_dim") \
    .mode("overwrite") \
    .save()

location_dim.write.format("bigquery") \
    .option("table", "linkedin_bq_dataset.location_dim") \
    .mode("overwrite") \
    .save()

date_dim.write.format("bigquery") \
    .option("table", "linkedin_bq_dataset.date_dim") \
    .mode("overwrite") \
    .save()