import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, DateType


spark = SparkSession.builder \
        .appName("linkedin-data-transforms") \
        .getOrCreate()

spark.conf.set('temporaryGcsBucket', 'dataproc-temp-bucket-gcs-linkedin-pipeline')

skills_schema = StructType([
    StructField('job_link', StringType(), False),
    StructField('job_skills', StringType(), True)
])

# summary_schema = StructType([
#     StructField('job_link', StringType(), False),
#     StructField('job_summary', StringType(), True)
# ])

postings_schema = StructType([
    StructField('job_link', StringType(), False),
    StructField('last_processed_time', DateType(), True),
    StructField('got_summary', BooleanType(), False),
    StructField('got_ner', BooleanType(), False),
    StructField('is_being_worked', BooleanType(), False),
    StructField('job_title', StringType(), True),
    StructField('company', StringType(), True),
    StructField('job_location', StringType(), True),
    StructField('first_seen', DateType(), True),
    StructField('search_city', StringType(), True),
    StructField('search_country', StringType(), True),
    StructField('search_position', StringType(), True),
    StructField('job_level', StringType(), True),
    StructField('job_type', StringType(), True),
])

skills_data_path = "gs://gcs-linkedin-data-bucket/downloaded_data/job_skills.csv"
# summary_data_path = "gs://gcs-linkedin-data-bucket/downloaded_data/job_summary.csv"
postings_data_path = "gs://gcs-linkedin-data-bucket/downloaded_data/linkedin_job_postings.csv"

skills_df = spark.read.csv(skills_data_path, header=True, schema=skills_schema)
# summary_df = spark.read.csv(summary_data_path, header=True, schema=summary_schema)
postings_df = spark.read.csv(postings_data_path, header=True, schema=postings_schema)

skills_df = skills_df.withColumnRenamed("job_skills", "skills_list")
postings_df = postings_df.withColumnRenamed('last_processed_time', 'last_processed_date') \
                        .withColumnRenamed('job_location', 'default_job_location') \
                        .withColumnRenamed('search_city', 'city') \
                        .withColumnRenamed('search_country', 'country') \
                        .withColumnRenamed('search_position', 'search_title')


columns_to_drop = ['got_ner', 'is_being_worked', 'job_level', 'job_type']
postings_df = postings_df.drop(*columns_to_drop)

skills_df.write.mode("overwrite").parquet("gs://gcs-linkedin-data-bucket/processed_data/skills.parquet")
# summary_df.write.mode("overwrite").parquet("gs://gcs-linkedin-data-bucket/processed_data/summary.parquet")
postings_df.write.mode("overwrite").parquet("gs://gcs-linkedin-data-bucket/processed_data/jobs.parquet")