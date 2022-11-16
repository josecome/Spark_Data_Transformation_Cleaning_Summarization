from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
spark = SparkSession.builder.appName('Data_Transformation').getOrCreate()

#Read data
ds = spark.read.csv('data.csv',header=True,inferSchema=True)

#Read data2
ds2 = spark.read.csv('data2.csv',header=True,inferSchema=True)

#Rename column id to id2
ds2 = ds2.withColumnRenamed('id','id2')

#Join dataset to have all needed columns
ds = ds.join(ds2, ds.id == ds2.id2, 'right').select(ds['*'],ds2['*'])

#Drop id column we will not need for analysis
ds = ds.drop('id2')

#Display data
ds.show()

#Display schema
ds.printSchema()

#Rename column sex to gender
ds = ds.withColumnRenamed('sex','gender')

#Dataset with no null values
ds_with_no_na = ds.na.drop(how="any")

#Filling the Missing Value
ds = ds.na.fill('0',['income_per_mth','savings_per_mth'])

#If need to remove row with null value in specific row ex: income_per_mth
ds = ds.na.drop(how="any",subset=['income_per_mth','savings_per_mth'])

#Adding column to show used money per month
ds = ds.withColumn("used_money_per_mth", ds.income_per_mth - ds.savings_per_mth)

#Selct only colum gender and income
ds.select(['gender','income_per_mth']).show()

#Group result by gender
ds.groupBy('gender').sum().show()

#Average by gender and ocupation
ds.groupBy('gender','ocupation').avg().show()

#Mean by gender
ds.groupBy('gender').mean().show()

#Count by gender
ds.groupBy('gender').count().show()

#Filters

#income equal or more than 400
ds.filter("income_per_mth>=400").show()

#income equal or more than 400 but only show colum gender and ocupation
ds.filter("income_per_mth>=400").select(['gender','ocupation']).show()

#income equal or less than 400
ds.filter("income_per_mth<=400").show()

#income between 400 to 800
ds.filter((ds['income_per_mth']>=400) | (ds['income_per_mth']<=800)).show()

#income different than 400 equal or less than 400
ds.filter(~(ds['income_per_mth']<=400)).show()




