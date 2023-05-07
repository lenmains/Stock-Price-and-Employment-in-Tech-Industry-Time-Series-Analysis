stock price stats
1) run "Clean.java" to clean up the row stock price data set
2) after the Clean program finishes, it will creates a file named part-r-00000 in the output folder. 
   use hdfs dfs -get to fetch it into local and use cat part-r-00000 > clean_data.csv to transform it into a new csv file
3) run you can run StatsSpecificYear with the clean data set to get the stock trend over years

employment analysis:
1) use "EmploymentClean.java" to clean up the file 
2) similarily, fetch the part-r-00000 file and cast it into a new csv file, e.g. ep_clean.csv
3) put it onto hdfs
5) create a new dir e.g. emp and store the clean csv file there
6) start hive terminal then create a table for ep_clean.csv using the commands in the code folder.

use sl8052_nyu_edu;

CREATE EXTERNAL TABLE emp(year string, employment double) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE location  'hdfs://nyu-dataproc-m/user/sl8052_nyu_edu/emp';

create table year_ep as select year, avg(employment) from t_ep group by year;

INSERT OVERWRITE DIRECTORY 'hdfs://nyu-dataproc-m/user/sl8052_nyu_edu/output' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE select * from year_ep;

7) after this, you will see a file named 000000_0 in the output folder, change the name to something more meaning for e.g. year_ep.csv

linear regreesion analysis with spark

* before you use the spark, make sure to add header "year,employment" into the csv file

1) open spark terminal
2) use the year_ep.csv file on hdfs
3) input the code in file "spark_linear_regression"


import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression

val spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

val df = spark.read.option("header", "true").csv("year_ep.csv")
val df1 = df.withColumn("emp_n", col("employment").cast("double"))

val df2 = df1.withColumn("year_n", col("year").cast ("int")).select ("year_n", "emp_n")
val assembler = new VectorAssembler().setInputCols(Array("year_n")).setOutputCol("new_col")

val lr_model = new LinearRegression().setLabelCol("emp_n"). setFeaturesCol ("new_col")
val df3 = assembler.transform(df2)
val lr_model1 = lr_model.fit(df3)

println(s"Coefficients: ${lr_model1.coefficients} Intercept: ${lr_model1.intercept}")
val trainingSummary = lr_model1.summary
trainingSummary.residuals.show()


println(s"r2: ${trainingSummary.r2}")
println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")


*Most files are in the root directories. Hive related csv files are located in their folders described above.
