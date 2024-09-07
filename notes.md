### Entire playlist for PySpark

https://www.youtube.com/watch?v=6MaZoOgJa84&list=PLMWaZteqtEaJFiJ2FyIKK0YEuXwQ9YIS_

⭐Please read about Apache Spark and Big data and why it came into existence from Week 9 before this.


# What is Pyspark
video link : https://youtu.be/6MaZoOgJa84

PySpark is an interface for Apache Spark in Python. It not only allows you to write spark applications using Python APIs, but also provides the PySpark shell for interactively analyzing your data in distributed environment. 

PySpark supports most Spark features such as **SparkSQL, DataFrame, Streaming, MLib (Machine Learning) and spark core**.

![](https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSePnamTescIkx7Ip9UQFcn-1XqHS3shqrtSQ&s)


# Create dataframe
video link : https://youtu.be/mIAIQI5rMY8

DataFrame is **distributed collection of data organized into named columns**.

Conceptually equivalent to tables in relational database.

⭐⭐see demo in video
![](./Screenshot%20(912).png)

### ⭐⭐[read stackoverflow](https://stackoverflow.com/questions/57959759/manually-create-a-pyspark-dataframe) and [official pyspark doc](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.SparkSession.createDataFrame.html) to know about dataframe creation

```py
df = spark.createDataFrame(
    [
        (1, "foo"),  # create your data here, be consistent in the types.
        (2, "bar"),
    ],
    ["id", "label"]  # add your column names here
)

df.printSchema() # kind of like desc in sql
```
Output
```
root
 |-- id: long (nullable = true)
 |-- label: string (nullable = true)
```

```py
df.show()
```
Output
```
+---+-----+                                                                 
| id|label|
+---+-----+
|  1|  foo|
|  2|  bar|
+---+-----+
```

### Basic syntax
```py
SparkSession.createDataFrame(
    data, 
    schema=None, 
    samplingRatio=None, 
    verifySchema=True)
```
### Parameters
1. `data` : **RDD or iterable**

    an **RDD** of any kind of SQL data representation(e.g. Row, tuple, int, boolean, etc.), or list, or pandas.DataFrame.

2. `schema` : `pyspark.sql.types.DataType`, `str` or `list`, **optional**
    
    a `pyspark.sql.types.DataType` or a datatype string or a list of column names, default is None. 
    
    The data type string format equals to `pyspark.sql.types.DataType.simpleString`, except that top level struct type can omit the `struct<>` and atomic types use `typeName()` as their format, e.g. use byte instead of `tinyint` for `pyspark.sql.types.ByteType`. 
    
    We can also use `int` as a short name for `pyspark.sql.types.IntegerType`.

3. `samplingRatio`: `float`, **optional**

    the sample ratio of rows used for inferring

4. `verifySchema` : `bool`, **optional**

    verify data types of every row against schema. Enabled by default.

### Returns
    DataFrame

### Defining DataFrame Schema with StructField and StructType

⭐⭐[read gfg blog here](https://www.geeksforgeeks.org/defining-dataframe-schema-with-structfield-and-structtype/)

### Create dataframe with dictionary

⭐⭐[Read gfg](https://www.geeksforgeeks.org/create-pyspark-dataframe-from-dictionary/)

# Read CSV File in to dataframe
video link : https://youtu.be/lRkIQMRXcYw

Use `csv("path")` or `format("csv").load("path")` of DataFrameReader, you can read a CSV file into a PySpark Dataframe.
![](./Screenshot%20(913).png)

⭐⭐see demo


# Write dataframe into CSV file
Video link : https://youtu.be/SQfTHPvzlEI

### ⭐⭐[read this](https://sparkbyexamples.com/pyspark/pyspark-write-dataframe-to-csv-file/) and [this](https://www.deeplearningnerds.com/pyspark-write-dataframe-to-csv-file/)

In PySpark you can save (write/extract) a DataFrame to a CSV file on disk by using `dataframeObj.write.csv("path")`, using this you can also write DataFrame to AWS S3, Azure Blob, HDFS, or any PySpark supported file systems.

### Write PySpark DataFrame to CSV File
we would like to write the already created PySpark DataFrame to a CSV file. The file should have the following attributes:

- File should include a header with the column names.
- Columns of the file should be separated with semi-colon ;.
- Existing file should be overwritten.
- File path should be "data/frameworks.csv".

We can do this in two different ways.

#### 1. using csv()
    
To do this, we first create a `DataFrameWriter` instance with `df.write`. Afterwards, we use the `csv()` method in combination with the `option()` method and the `mode()` method of `DataFrameWriter`:
```py
df.write.option("header",True) \
    .option("delimiter",";") \
    .mode("overwrite") \
    .csv("data/frameworks.csv")
```
#### 2. using format("csv").save()
Now, we consider another option to write the PySpark DataFrame to a CSV file.

First, we create a `DataFrameWriter` instance with `df.write`. Afterwards, we use the `save()` method in combination with the `format()` method, the `option()` method and the `mode()` method of `DataFrameWriter`:
```py
df.write.option("header",True) \
    .option("delimiter",";") \
    .format("csv") \
    .mode("overwrite") \
    .save("data/frameworks.csv")
```

# Read json File in to dataframe
Video link : https://youtu.be/HdfQWt3DgW0

![](./Screenshot%20(914).png)
![](./Screenshot%20(915).png)
![](./Screenshot%20(916).png)

### ⭐⭐See video


# Write dataframe into json file
Video link : https://youtu.be/U0iwA473r1c?si=olEYeZxAEFQJvnq9

Use `DataFrameWriter` object to write PySpark DataFrame to a CSV file.

```py
df.write.json(path='dbfs:/FileStore/data/jsonemps')
```

### ⭐⭐see demo in video

# Read parquet file in to dataframe
Video link : https://youtu.be/VeeJuNsTjmg?si=VyOokrKOlbwpUR7f

![](./Screenshot%20(917).png)
![](./Screenshot%20(918).png)

### ⭐⭐see demo in video

# Write dataframe into parquet
Video link : https://youtu.be/Ck8pEx6WafQ?si=TideguMq8SmSPFYs 

![](./Screenshot%20(919).png)
![](./Screenshot%20(920).png)

### ⭐⭐see demo in video

# show() in pyspark
Video link : https://youtu.be/9VhitO4KFv0

`show()` displays contents of the table.

![](./Screenshot%20(921).png)

### ⭐⭐see demo in video

# withColumn() in pyspark
Video link : https://youtu.be/RgGT7LfHBQs

![](./Screenshot%20(922).png)

### ⭐⭐see demo in video

# withColumnRenamed() in pyspark
Video link : https://youtu.be/z2_ajv_aY2Y

used to rename column name in dataframe.

![](./Screenshot%20(923).png)

### ⭐⭐see demo in video

# StructType() & StructField()
Video link : https://youtu.be/D0Xoyd7rpV0?si=0Gn6TOF7GU-Y8ijN

![](./Screenshot%20(924).png)
![](./Screenshot%20(925).png)

### ⭐⭐see demo in video

# ArrayType Columns in PySpark

Video link : https://youtu.be/jN5kRJ0TOf4?si=BYX6LT8QfK4lRtK8

![](./Screenshot%20(926).png)
![](./Screenshot%20(927).png)

### ⭐⭐see demo in video

# explode(),split(),array_contains(),array() functions

Video link : https://youtu.be/DSiIiDv3fMQ?si=-pIJHjqvr_trup96

![](./Screenshot%20(928).png)
![](./Screenshot%20(930).png)
![](./Screenshot%20(931).png)
![](./Screenshot%20(932).png)

### ⭐⭐see demo in video

# Maptype
Video link : https://youtu.be/k5K3L9c2HaE?si=JYO_Qwj8GWW-dxUx

![](./Screenshot%20(933).png)
![](./Screenshot%20(934).png)

### ⭐⭐see demo in video

<span style="color:yellow"><Video 16 in the series skipped></span>


# row() class in pyspark
Video link : https://youtu.be/Xb592yRvV8A?si=FCf57qHYezJms6DU

![](./Screenshot%20(935).png)
![](./Screenshot%20(936).png)
![](./Screenshot%20(937).png)

### ⭐⭐see demo in video
<span style="color:yellow"><Video 18 in the series skipped></span>

# when() & otherwise() functions
Video link : https://youtu.be/a0KDOOcN4Oc?si=W46CWaaxGinsiPyM

![](./Screenshot%20(938).png)

### ⭐⭐see demo in video

# alias(), asc(), desc(), cast() & like() 
Video link : https://youtu.be/cDLW_GzvMSA?si=AeCwShOVWKT7FE-m

![](./Screenshot%20(940).png)
![](./Screenshot%20(941).png)

### ⭐⭐see demo in video

# filter() & where() functions
Video link : https://youtu.be/YHWIGvBjCNc?si=pSe_jNHW2slHJlTg

![](./Screenshot%20(939).png)

### ⭐⭐see demo in video

# distinct()& dropDuplicates()
Video link : https://youtu.be/YwvGkA9L92c?si=DRZzLQpWlsdw0EgN

![](./Screenshot%20(942).png)

### ⭐⭐see demo in video

# orderBy() & sort()
Video link : https://youtu.be/wuxhIe6WzSU?si=aCuNICn4Thht_R2V

![](./Screenshot%20(943).png)

### ⭐⭐see demo in video

# union& unionAll()
Video link : https://youtu.be/yt1HAaoUQbg?si=TNCqmFiH_kxuhwAq

![](./Screenshot%20(944).png)

### ⭐⭐see demo in video

# groupBy()
Video link : https://youtu.be/IQVMZKjrIH0?si=AHlDjscEbZGCGvA3

![](./Screenshot%20(945).png)

### ⭐⭐see demo in video

# groupBy agg()
Video link :  https://youtu.be/wRHfkdh4s60?si=Ovcf9CsUT2Wv9OLj

![](./Screenshot%20(946).png)

### ⭐⭐see demo in video

# UnionByName()
Video link : https://youtu.be/CWClMij6KHQ?si=h_Z2qmBnIoXvADxk

![](./Screenshot%20(947).png)

### ⭐⭐see demo in video

# select()
Video link : https://youtu.be/IR9nX5lS924?si=bUKKzMbCto2dijIC

![](./Screenshot%20(948).png)

### ⭐⭐see demo in video

# joins part 1
Video link : https://youtu.be/6mWi5T7SAug?si=r6mjACEjebOH9ijm

![](./Screenshot%20(949).png)

### ⭐⭐see demo in video

# joins part 2
Video link : https://youtu.be/3hynmMg80Nk?si=s4Yihk6FDi0Mk0gq

![](./Screenshot%20(950).png)

### ⭐⭐see demo in video

# pivot
Video link : https://www.youtube.com/watch?v=pmPLyMweDSo&t=37s

![](./Screenshot%20(951).png)

### ⭐⭐see demo in video

# unpivot
Video link : https://www.youtube.com/watch?v=vi4G7N7msh4

![](./Screenshot%20(952).png)

### ⭐⭐see demo in video

# fill()& fillna()
Video link : https://www.youtube.com/watch?v=YuTKuqOlntw

![](./Screenshot%20(953).png)

### ⭐⭐see demo in video

<span style="color:yellow"><Video 34 in the series skipped></span>

# collect()
Video link : https://www.youtube.com/watch?v=eID3bi5_lgM

![](./Screenshot%20(954).png)

### ⭐⭐see demo in video

<span style="color:yellow"><Videos 36 and 37 in the series skipped></span>

# createOrReplaceTempView()
Video link : https://www.youtube.com/watch?v=SMuq5zar6Bw

![](./Screenshot%20(955).png)

### ⭐⭐see demo in video

<span style="color:yellow"><Video 39 in the series skipped></span>

# UDF
Video link : https://www.youtube.com/watch?v=deJ0XQfDLFM

![](./Screenshot%20(956).png)
![](./Screenshot%20(957).png)
![](./Screenshot%20(958).png)

### ⭐⭐see demo in video

# convert RDD to dataframe
Video link : https://www.youtube.com/watch?v=7R_-_K7HxZw

![](./Screenshot%20(960).png)
![](./Screenshot%20(961).png)

### ⭐⭐see demo in video


<span style="color:yellow"><Videos 42, 43 in the series skipped></span>

# partitionBy()
Video link : https://www.youtube.com/watch?v=JFVl-9YB84U

![](./Screenshot%20(959).png)

### ⭐⭐see demo in video


# Timestamp Functions
Video link : https://youtu.be/a7pAOHf-9ko?si=nzf0S5kpxZ1t0BDp

![](./Screenshot%20(962).png)

### ⭐⭐see demo in video

<span style="color:yellow"><Video 53 in the series skipped></span>

# Window Functions
Video link : https://youtu.be/fxh0CSVBlbk?si=abkjp_H4sRd-furM

![](./Screenshot%20(963).png)

### ⭐⭐see demo in video