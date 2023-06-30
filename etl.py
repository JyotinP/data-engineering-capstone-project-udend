from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SparkMisc').getOrCreate()
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, TimestampType ,FloatType

def create_spark_session():
    spark = SparkSession.builder \
        .appName('Capstone_Proj_1') \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport().getOrCreate()
    return spark


def process_immigration_and_time_data(spark, input_data, output_data):
     
    df_immigration = spark.read.parquet(input_data)

    df_immigration = df_immigration.select('i94yr','i94mon','i94res','i94port','arrdate','i94mode','i94addr' \
                        ,'depdate','i94bir','i94visa','biryear','gender','airline','visatype')
     
    df_immigration = df_immigration.withColumnRenamed('i94yr', 'year') \
                                    .withColumnRenamed('i94mon', 'month') \
                                    .withColumnRenamed('i94res', 'ctry_of_res_code') \
                                    .withColumnRenamed('i94port', 'port_code') \
                                    .withColumnRenamed('arrdate', 'arrvl_dt_sas') \
                                    .withColumnRenamed('i94mode', 'mode_code') \
                                    .withColumnRenamed('i94addr', 'dest_state_code') \
                                    .withColumnRenamed('depdate', 'depart_dt_sas') \
                                    .withColumnRenamed('i94bir', 'age') \
                                    .withColumnRenamed('i94visa', 'visa_code') \
                                    .withColumnRenamed('biryear', 'dob') \
                                    .withColumnRenamed('visatype', 'visa_type')

    df_immigration = df_immigration.filter(F.col("dest_state_code").isNotNull())

    df_immigration = df_immigration.filter(F.col("gender").isNotNull())

    df_immigration.createOrReplaceTempView("immigration_table")
    df_immigration = spark.sql("SELECT *, date_add(to_date('1960-01-01'), int(arrvl_dt_sas)) AS arrvl_dt FROM immigration_table")

    df_immigration.createOrReplaceTempView("immigration_table")
    df_immigration = spark.sql("SELECT *, date_add(to_date('1960-01-01'), int(depart_dt_sas)) AS depart_dt FROM immigration_table")

    df_immigration = df_immigration.drop("arrvl_dt_sas","depart_dt_sas")

    fact_immigration = df_immigration.alias('fact_immigration')

    fact_immigration = fact_immigration.withColumn("year", F.col("year").cast(IntegerType())) \
                                        .withColumn("month", F.col("month").cast(IntegerType())) \
                                        .withColumn("ctry_of_res_code", F.col("ctry_of_res_code").cast(IntegerType())) \
                                        .withColumn("mode_code", F.col("mode_code").cast(IntegerType())) \
                                        .withColumn("age", F.col("age").cast(IntegerType())) \
                                        .withColumn("visa_code", F.col("visa_code").cast(IntegerType())) \
                                        .withColumn("dob", F.col("dob").cast(IntegerType()))

    fact_immigration.write.partitionBy("year", "month") \
            .parquet(output_data + "fact_immigration/")
    
    dim_date = df_immigration.select(F.col("arrvl_dt"),
                            F.year(df_immigration["arrvl_dt"]).alias("year"),
                            F.month(df_immigration["arrvl_dt"]).alias("month"),
                            F.weekofyear(df_immigration["arrvl_dt"]).alias("week_of_year"),
                            F.dayofyear(df_immigration["arrvl_dt"]).alias("day_of_year"),
                            F.dayofmonth(df_immigration["arrvl_dt"]).alias("day_of_month"),
                            F.dayofweek(df_immigration["arrvl_dt"]).alias("day_of_week"),
                            F.quarter(df_immigration["arrvl_dt"]).alias("quarter")
                           ).dropDuplicates(["arrvl_dt"])
    
    dim_date.write.partitionBy("year", "month") \
            .parquet(output_data + "dim_date/")
    

def process_demographics_data(spark, input_data, output_data):

    df_demographics = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_demographics = df_demographics.groupBy(df_demographics["State Code"].alias("state_code")).agg(
    F.avg("Median Age").cast(FloatType()).alias("medn_age"),
    F.sum("Male Population").cast(IntegerType()).alias("male_pop"),
    F.sum("Female Population").cast(IntegerType()).alias("female_pop"),
    F.sum("Total Population").cast(IntegerType()).alias("total_pop"),
    F.sum("Number of Veterans").cast(IntegerType()).alias("vet_cnt"),
    F.sum("Foreign-born").cast(IntegerType()).alias("foreign_born"),
    F.avg("Average Household Size").cast(FloatType()).alias("avg_hsehld_size")
    )

    dim_demographics.write \
            .parquet(output_data + "dim_demographics/") 
    
    
def process_country_data(spark, input_data, output_data):

    country_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_country = country_label.alias('dim_country')

    dim_country = dim_country.withColumn("country_code", F.col("country_code").cast(IntegerType()))

    dim_country.write \
            .parquet(output_data + "dim_country/") 


def process_port_data(spark, input_data, output_data):

    port_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_port = port_label.alias('dim_port')

    dim_port.write \
            .parquet(output_data + "dim_port/") 
    

def process_mode_data(spark, input_data, output_data):

    mode_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_mode = mode_label.alias('dim_mode')

    dim_mode = dim_mode.withColumn("mode_code", F.col("mode_code").cast(IntegerType()))

    dim_mode.write \
            .parquet(output_data + "dim_mode/")
    

def process_state_data(spark, input_data, output_data):

    state_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_state = state_label.alias('dim_state')

    dim_state.write \
            .parquet(output_data + "dim_state/")

    
def process_visa_data(spark, input_data, output_data):

    visa_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_visa = visa_label.alias('dim_visa')

    dim_visa = dim_visa.withColumn("visa_code", F.col("visa_code").cast(IntegerType()))

    dim_visa.write \
            .parquet(output_data + "dim_visa/")


def main():
    spark = create_spark_session()
    input_data = {
        "immigration" :  r"data/sas_data",
        "demographics" : r"data/us-cities-demographics.csv" ,
        "country" : r"data/I94_country_label.csv",
        "port" : r"data/I94_port_label.csv" ,
        "mode" : r"data/I94_mode_label.csv" ,
        "state" : r"data/I94_state_label.csv" ,
        "visa" : r"data/I94_visa_label.csv"
    }

    output_data = r"data/parquet_write/"
    
    process_immigration_and_time_data(spark, input_data['immigration'], output_data)    
    process_demographics_data(spark, input_data['demographics'], output_data)
    process_country_data(spark, input_data['country'], output_data)    
    process_port_data(spark, input_data['port'], output_data)
    process_mode_data(spark, input_data['mode'], output_data)    
    process_state_data(spark, input_data['state'], output_data)
    process_visa_data(spark, input_data['visa'], output_data)

if __name__ == "__main__":
    main()




