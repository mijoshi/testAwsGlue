# testAwsGlue

AWS Glue has a few limitations on the transformations such as UNION, LEFT JOIN, RIGHT JOIN, etc. To overcome this issue, we can use Spark. Convert Dynamic Frame of AWS Glue to Spark DataFrame and then you can apply Spark functions for various transformations.

## Convert Glue Dynamic frame to Spark DataFrame
spark_data_frame_1 = glue_dynamic_frame_1.toDF()
spark_data_frame_2 = glue_dynamic_frame_2.toDF()
## Apply UNION Transformation on Spark DataFrame
spark_data_frame_union = spark_data_frame_1.union(spark_data_frame_2).distinct()
## Again, convert Spark DataFrame back to Glue Dynamic Frame
glue_dynamic_frame_union = DynamicFrame.fromDF(spark_data_frame_union, glueContext, "spark_data_frame_union")
