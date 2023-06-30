from pyspark.sql import SparkSession
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType, TimestampType ,FloatType, StringType

def create_spark_session():
    spark = SparkSession.builder \
        .appName('Capstone_Proj_1') \
        .config("spark.jars.repositories", "https://repos.spark-packages.org/") \
        .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11") \
        .enableHiveSupport().getOrCreate()
    return spark


def qc_immigration_and_time_data(spark, input_data):
     
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
    
    if fact_immigration.count() > 0 :
        print("Count Check Successful for fact_immigration")

    if fact_immigration.schema["year"].dataType == IntegerType():
        print("Integrity Check Successfull for fact_immigration") 
    
    dim_date = df_immigration.select(F.col("arrvl_dt"),
                            F.year(df_immigration["arrvl_dt"]).alias("year"),
                            F.month(df_immigration["arrvl_dt"]).alias("month"),
                            F.weekofyear(df_immigration["arrvl_dt"]).alias("week_of_year"),
                            F.dayofyear(df_immigration["arrvl_dt"]).alias("day_of_year"),
                            F.dayofmonth(df_immigration["arrvl_dt"]).alias("day_of_month"),
                            F.dayofweek(df_immigration["arrvl_dt"]).alias("day_of_week"),
                            F.quarter(df_immigration["arrvl_dt"]).alias("quarter")
                           ).dropDuplicates(["arrvl_dt"])
    
    if dim_date.count() > 0 :
        print("Count Check Successful for dim_date")

    if dim_date.schema["year"].dataType == IntegerType():
        print("Integrity Check Successfull for dim_date") 
    
    
def qc_demographics_data(spark, input_data):

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

    if dim_demographics.count() > 0 :
        print("Count Check Successful for dim_demographics")

    if dim_demographics.schema["state_code"].dataType == StringType():
        print("Integrity Check Successfull for dim_demographics") 

    
def qc_country_data(spark, input_data):

    country_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_country = country_label.alias('dim_country')

    dim_country = dim_country.withColumn("country_code", F.col("country_code").cast(IntegerType()))

    if dim_country.count() > 0 :
        print("Count Check Successful for dim_country")

    if dim_country.schema["country_code"].dataType == IntegerType():
        print("Integrity Check Successfull for dim_country") 


def qc_port_data(spark, input_data):

    port_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_port = port_label.alias('dim_port')

    if dim_port.count() > 0 :
        print("Count Check Successful for dim_port")

    if dim_port.schema["port_code"].dataType == StringType():
        print("Integrity Check Successfull for dim_port") 
    

def qc_mode_data(spark, input_data):

    mode_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_mode = mode_label.alias('dim_mode')

    dim_mode = dim_mode.withColumn("mode_code", F.col("mode_code").cast(IntegerType()))

    if dim_mode.count() > 0 :
        print("Count Check Successful for dim_mode")

    if dim_mode.schema["mode_code"].dataType == IntegerType():
        print("Integrity Check Successfull for dim_mode") 

    
def qc_state_data(spark, input_data):

    state_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_state = state_label.alias('dim_state')

    if dim_state.count() > 0 :
        print("Count Check Successful for dim_state")

    if dim_state.schema["state_code"].dataType == StringType():
        print("Integrity Check Successfull for dim_state") 

    
def qc_visa_data(spark, input_data):

    visa_label = spark.read.options(delimiter=';').csv(input_data, header=True)

    dim_visa = visa_label.alias('dim_visa')

    dim_visa = dim_visa.withColumn("visa_code", F.col("visa_code").cast(IntegerType()))

    if dim_visa.count() > 0 :
        print("Count Check Successful for dim_visa")

    if dim_visa.schema["visa_code"].dataType == IntegerType():
        print("Integrity Check Successfull for dim_visa") 


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

    qc_immigration_and_time_data(spark, input_data['immigration'])    
    qc_demographics_data(spark, input_data['demographics'])
    qc_country_data(spark, input_data['country'])    
    qc_port_data(spark, input_data['port'])
    qc_mode_data(spark, input_data['mode'])    
    qc_state_data(spark, input_data['state'])
    qc_visa_data(spark, input_data['visa'])

if __name__ == "__main__":
    main()




