# json_parse.py
import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession.builder.appName("sample").master("local[*]").getOrCreate()

def main():
    db_list = read_json_file()[0]
    df_list = read_json_file()[1]
    print("write table start!")
    write_table(db_list,df_list)
    print("write table ended!")
    print("write column_start!")
    write_column(db_list, df_list)
    print("write column ended!")
    return


"""
#def get_sparksession():
#    spark = SparkSession.builder.appName("sample").master("local[*]").getOrCreate()
#    return spark
"""


def read_json_file() -> list:
    database_list = ["DB_NAME"]
    dataframe_list = []
    for i in database_list:
        df = spark.read.format("json").option("inferSchema","true").option("multiline","true").load(f"C:/Users/jiho3/Downloads/{i}.json")
        dataframe_list.append(df)
    return database_list, dataframe_list


def write_table(db_list: list, df_list: list):
    for i in range(len(df_list)):
        try:# table comment가 있는 경우
            parse_df_json_table = df_list[i].withColumn("flatten", F.explode("TableList")) \
                                            .withColumn("No", F.monotonically_increasing_id())\
                                            .withColumn("Table_name", F.col("flatten").getItem("Name")) \
                                            .withColumn("Parameters", F.col("flatten").getItem("Parameters"))\
                                            .withColumn("Table_comment", F.when(F.trim(F.col("Parameters").getItem("comment")) == "", F.lit("-"))
                                                                          .when(F.col("Parameters").getItem("comment").isNull(),F.lit("-"))
                                                                          .otherwise(F.col("Parameters").getItem("comment"))) \
                                            .withColumn("description", F.col("Table_Name")) \
                                            .select(F.col("No"),
                                                    F.lit("신규").alias("reqeust_type"),
                                                    F.lit("DB_name").alias("db_name"),
                                                    F.lit("subject_area_name").alias("subject_area_name"),
                                                    F.lit("Schema_name").alias("schema_name"),
                                                    F.col("Table_name"),
                                                    F.col("Table_comment"),
                                                    F.lit("").alias("occurence_cycle"),
                                                    F.lit("").alias("perservation_period"),
                                                    F.lit("").alias("table_volume"),
                                                    F.lit("").alias("partition"),
                                                    F.lit("").alias("partition_column"),
                                                    F.lit("").alias("PK_partition"),
                                                    F.col("description"),
                                                    F.lit("N").alias("standard_yn"))
        except:# table comment가 없는 경우
            parse_df_json_table = df_list[i].withColumn("flatten", F.explode("TableList")) \
                                            .withColumn("No", F.monotonically_increasing_id()) \
                                            .withColumn("Table_name", F.col("flatten").getItem("Name")) \
                                            .withColumn("Table_comment", F.lit("-")) \
                                            .withColumn("description", F.col("Table_Name")) \
                                            .select(F.col("No"),
                                                    F.lit("신규").alias("reqeust_type"),
                                                    F.lit("DB_name").alias("db_name"),
                                                    F.lit("subject_area_name").alias("subject_area_name"),
                                                    F.lit("Schema_name").alias("schema_name"),
                                                    F.col("Table_name"),
                                                    F.col("Table_comment"),
                                                    F.lit("").alias("occurence_cycle"),
                                                    F.lit("").alias("perservation_period"),
                                                    F.lit("").alias("table_volume"),
                                                    F.lit("").alias("partition"),
                                                    F.lit("").alias("partition_column"),
                                                    F.lit("").alias("PK_partition"),
                                                    F.col("description"),
                                                    F.lit("N").alias("standard_yn"))

        parse_df_json_table.coalesce(1).write.mode("overwrite").format("csv")\
                                       .option("encoding","euc-kr").option("header", "true").save(f"C:/Users/JIHO PARK/Downloads/s3 csv파일/s3테이블/{db_list[i]}") # euc-kr로 한글 인코딩
    return

def write_column(db_list: list, df_list: list):
    for i in range(len(df_list)):
        try:# column comment가 있는 경우
            parse_df_json_column = df_list[i].withColumn("flatten", F.explode("TableList")) \
                                             .withColumn("No", F.monotonically_increasing_id()) \
                                             .withColumn("Table_name",F.col("flatten").getItem("Name"))\
                                             .withColumn("Columns_nf", F.col("flatten").getItem("StorageDescriptor").Columns)\
                                             .withColumn("Columns_f", F.explode("Columns_nf"))\
                                             .withColumn("Column_name", F.col("Columns_f").Name)\
                                             .withColumn("Column_comment", F.col("Columns_f").Comment) \
                                             .withColumn("Column_seq", F.row_number().over(Window.partitionBy(F.col("Table_name")).orderBy(F.col("Column_name"))))\
                                             .withColumn("Column_type_A", F.col("Columns_f").Type)\
                                             .withColumn("Column_type", F.split(F.col("Column_type_A"), "\\(").getItem(0))\
                                             .withColumn("Column_length", F.regexp_replace(F.split(F.col("Column_type_A"), "\\(").getItem(1),"\\)",""))\
                                             .withColumn("Description", F.col("Column_name"))\
                                             .select(F.col("No"),
                                                     F.lit("신규").alias("reqeust_type"),
                                                     F.lit("DB_name").alias("db_name"),
                                                     F.lit("subject_area_name").alias("subject_area_name"),
                                                     F.lit("Schema_name").alias("schema_name"),
                                                     F.col("Table_name"),
                                                     F.col("Column_name"),
                                                     F.when(F.trim(F.col("Column_comment")) == "", F.lit("-"))
                                                      .when(F.col("Column_comment").isNull(),F.lit("-"))
                                                      .otherwise(F.col("Column_comment")).alias("Column_comment"),
                                                     F.col("Column_seq"),
                                                     F.col("Column_type"),
                                                     F.col("Column_length"),
                                                     F.lit("N").alias("PK_yn"),
                                                     F.lit("N").alias("Null_allow_yn"),
                                                     F.col("Description"))
        except:# column comment가 없는 경우
            parse_df_json_column = df_list[i].withColumn("flatten", F.explode("TableList")) \
                                             .withColumn("No", F.monotonically_increasing_id()) \
                                             .withColumn("Table_name", F.col("flatten").getItem("Name")) \
                                             .withColumn("Columns_nf", F.col("flatten").getItem("StorageDescriptor").Columns) \
                                             .withColumn("Columns_f", F.explode("Columns_nf")) \
                                             .withColumn("Column_name", F.col("Columns_f").Name) \
                                             .withColumn("Column_seq", F.row_number().over(Window.partitionBy(F.col("Table_name")).orderBy(F.col("Column_name")))) \
                                             .withColumn("Column_type_A", F.col("Columns_f").Type) \
                                             .withColumn("Column_type", F.split(F.col("Column_type_A"), "\\(").getItem(0)) \
                                             .withColumn("Column_length",
                                                         F.regexp_replace(F.split(F.col("Column_type_A"), "\\(").getItem(1), "\\)", "")) \
                                             .withColumn("Description", F.col("Column_name")) \
                                             .select(F.col("No"),
                                                     F.lit("신규").alias("reqeust_type"),
                                                     F.lit("DB_name").alias("db_name"),
                                                     F.lit("subject_area_name").alias("subject_area_name"),
                                                     F.lit("Schema_name").alias("schema_name"),
                                                     F.col("Table_name"),
                                                     F.col("Column_name"),
                                                     F.lit("-").alias("Column_comment"),
                                                     F.col("Column_seq"),
                                                     F.col("Column_type"),
                                                     F.col("Column_length"),
                                                     F.lit("N").alias("PK_yn"),
                                                     F.lit("N").alias("Null_allow_yn"),
                                                     F.col("Description"))

        parse_df_json_column.coalesce(1).write.mode("overwrite").format("csv")\
                                        .option("encoding","euc-kr").option("header", "true").save(f"C:/Users/JIHO PARK/Downloads/s3 csv파일/s3컬럼/{db_list[i]}") # euc-kr로 한글 인코딩
    return
# split은 \\( 해야 가능하다
# raw_dhub, sdhub 나눠서 처리 try, except로 나눔
# monotonically_increasing_id를 사용하면 데이터 프레임의 로우에 할당된 고유 ID를 출력