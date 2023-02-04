import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import Window


def main(spark, exec_date):
    read_path = f'/covid_data/csv/{exec_date}.csv'
    save_path = f'/covid_data/results/exec_date={exec_date}'

    w = Window.partitionBy()

    df = (
        spark.read.option('header', True).csv(read_path)
        .groupBy(F.col('country_region'))
        .agg(F.sum(F.col('confirmed').cast(T.IntegerType())).alias('total_confirmed'),
             F.sum(F.col('deaths').cast(T.IntegerType())).alias('total_deaths'))
        .withColumn('fatality_ratio', F.col('total_deaths') / F.col('total_confirmed'))
        .withColumn('world_case_pct', F.col('total_confirmed') / F.sum(F.col('total_confirmed')).over(w))
        .withColumn('world_death_pct', F.col('total_deaths') / F.sum(F.col('total_deaths')).over(w))
    )

    df.repartition(1).write.mode('overwrite').format('csv').save(save_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--exec_date', required=True, type=str, help='Execution data')
    args = parser.parse_args()

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    try:
        main(spark, args.exec_date)
    finally:
        spark.stop()
