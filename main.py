import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import current_date, date_format, to_date
from pyspark.sql.functions import current_timestamp, to_timestamp, hour, minute, second, lit, date_trunc 

spark = SparkSession.builder.appName("practise").getOrCreate()
spark

df_pyspark = spark.read.csv('/Users/bharath/Desktop/workspace/pyspark/data/examp.csv', header=True, inferSchema=True, sep=';')

##Chechk the Schema
##print(type(df_pyspark))

##top rows
#print(df_pyspark.head(3))

##Selecting particular columns
#df_pyspark.select(["Name ", "Age"]).show()

##data types
#print(df_pyspark.dtypes)

##Summary
#df_pyspark.describe().show()

##Adding columns
#df_pyspark=df_pyspark.withColumn('Age after 2 years', col('Age')+2)
#df_pyspark.show()

##Renaming
#df_pyspark.withColumnRenamed('Name ', 'Name').show()

#Dropping
#df_pyspark.drop('Age after 2 years')

#MISSING Values
#df_pyspark.na.drop().show()

#df_pyspark.na.drop(how='any', subset=['Age']).show()

#filling the missing values
#df_pyspark.na.fill('Missing Value').show()

##Filter opertations
#df_pyspark.filter("Age >= 20").show()
##df_pyspark.filter("`Weight(KG)` >= 75").select("Name ", "Age").show()


#df_pyspark=df_pyspark.na.fill(value=66, subset=["Weight(KG)"])

#df_pyspark=df_pyspark.na.fill(value=20, subset=["Age"])

#df_pyspark=df_pyspark.na.fill(value="Unknown" , subset=["Name "])

#df_pyspark=df_pyspark.withColumn("Date ", current_date())

#df_pyspark=df_pyspark.withColumn("Date & Time", current_timestamp())
#df_pyspark=df_pyspark.withColumn("Date", to_date(df_pyspark["Date & Time"]))
#df_pyspark.show(truncate = False)
#df_pyspark = df_pyspark.withColumn("Yestraday Date & Time", to_timestamp(lit('2024.09.15 12.45.40'), 'yyyy.MM.dd HH.mm.ss'))
#df_pyspark.show(truncate = False)



df = df_pyspark.select(['Name ', 'Age'])
df.show()