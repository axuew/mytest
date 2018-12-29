from pyspark.sql import SparkSession
from pyspark import SparkConf, SQLContext
import json
import pandas as pd

spark = SparkSession.builder.appName("abv").getOrCreate()  # 创建spark对象
host = '192.168.4.94'
table = 'student'
conf = {"hbase.zookeeper.quorum": host, "hbase.mapreduce.inputtable": table}
keyConv = "org.apache.spark.examples.pythonconverters.ImmutableBytesWritableToStringConverter"
valueConv = "org.apache.spark.examples.pythonconverters.HBaseResultToStringConverter"
hbase_rdd = spark.sparkContext.newAPIHadoopRDD("org.apache.hadoop.hbase.mapreduce.TableInputFormat",
                                               "org.apache.hadoop.hbase.io.ImmutableBytesWritable",
                                               "org.apache.hadoop.hbase.client.Result", keyConverter=keyConv,
                                               valueConverter=valueConv, conf=conf)
# print(hbase_rdd.count())
# print(hbase_rdd.collect())





# print(spark.createDataFrame(output).show())
def call_transfor(y1):
    sb = {}
    y2 = [json.loads(i) for i in y1]
    sa = pd.DataFrame(y2)[['qualifier', 'value']]
    sb = dict(zip(sa['qualifier'], sa['value']))
    # print(sb)
    return sb
    # fdc = {}
    # for i in y2:
    #     colname = i['qualifier']
    #     value = i['value']
    #     fdc[colname] = value
    #     return fdc


def rdd_to_df(hbase_rdd):
    fdc_split = hbase_rdd.map(lambda x: (x[0], x[1].split('\n')))
    fdc_cols = fdc_split.map(lambda x: (x[0], call_transfor(x[1])))
    colnames = ['row_key'] + fdc_cols.map(lambda x: [i for i in x[1]]).take(1)[0]
    fdc_dataframe = fdc_cols.map(lambda x: [x[0]] + [x[1][i] for i in x[1]]).toDF(colnames)
    return fdc_dataframe


fdc_data = rdd_to_df(hbase_rdd)
print(fdc_data.show())
# output = hbase_rdd.flatMapValues(lambda v: v.split("\n")).mapValues(json.loads)
# fdc_cols = output.map(lambda x: (x[0], call_transfor(x[1])))
# print(output.collect())