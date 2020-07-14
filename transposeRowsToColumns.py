# Source: https://stackoverflow.com/questions/37864222/transpose-column-to-row-with-spark

from itertools import chain
from pyspark.sql import DataFrame
from pyspark.sql import (
    SparkSession
)
import findspark

findspark.init('/path/to/spark/apache-spark/2.4.5/libexec')

spark = SparkSession.builder \
        .appName('DataCleaning') \
        .master('local[*]') \
        .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer') \
        .getOrCreate()


def _sort_transpose_tuple(tup):
    x, y = tup
    return x, tuple(zip(*sorted(y, key=lambda v_k: v_k[1], reverse=False)))[0]


def transpose(X):

    cols = X.columns
    n_features = len(cols)

    result = X.rdd \
        .flatMap(lambda xs: chain(xs)) \
        .zipWithIndex() \
        .groupBy(lambda val_idx: val_idx[1] % n_features) \
        .sortBy(lambda grp_res: grp_res[0]) \
        .map(lambda grp_res: _sort_transpose_tuple(grp_res)) \
        .map(lambda x : x[1]) \
        .toDF()

    return result

df = spark.read.format("csv").option("header", "true").load("data.csv")
df.show()

newDf = transpose(df)
newDf.show()

nextDf = transpose(newDf)
nextDf.show()
