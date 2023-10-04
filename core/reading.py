from pyspark.sql import SparkSession
from constants import EnvVar
import glob
from lib.logger import Log4j
from lib.utils import get_spark_app_config


def reading_csv():
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    logger = Log4j(spark)
    # conf_out = spark.sparkContext.getConf()
    # logger.info(conf_out.toDebugString())
    logger.info("Starting reading")
    # Use glob to get a list of file paths that contain "20" in their names
    matching_files = glob.glob(f"{EnvVar.directory_path}/*20*.csv")

    # Read only the matching CSV files into a single DataFrame
    dfs = []
    for file_path in matching_files:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        dfs.append(df)

    # combining into single dataframe
    combined_df = None
    for df in dfs:
        if combined_df is None:
            combined_df = df
        else:
            combined_df = combined_df.union(df)

    logger.info("Finishing reading")
    # Stop the SparkSession when done
    return combined_df
