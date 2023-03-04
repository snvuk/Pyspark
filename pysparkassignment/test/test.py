import unittest
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pysparkassignment.core.util import *


class SparkETLTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[*]").appName("PySpark-unit-test").config('spark.port.maxRetries', 30).getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_etl(self):
        # here wrote Schema for input
        input_Schema = StructType([
            StructField("Product_Name ", StringType(), True),
            StructField("IssueDate", StringType(), True),
            StructField("Price", LongType(), True),
            StructField("Brand", StringType(), True),
            StructField("user_Country", StringType(), True),
            StructField("Product_number  ", StringType(), True)
        ])
    # here given input as data in manual
        input_data = [("Washing Machine", "1648770933000",20000, "Samsung", "india","001"),
                      ("Refrigerator", "1648770999000",35000, " LG", "null","002"),
                      ("Air Cooler", "1648770948000",45000, "  Voltas", "null","003")]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_Schema)

        # Unit Test Case Convert the Issue Date with the timestamp format
        # here expected out put Schema
        expected_schema = StructType([

            StructField("Product_Name ", StringType(), True),
            StructField("IssueDate", StringType(), True),
            StructField("Price", LongType(), True),
            StructField("Brand ", StringType(), True),
            StructField("user_Country", StringType(), True),
            StructField("Product_number  ", StringType(), True),
            StructField("equal_time", StringType(), True),
            StructField("date  ", StringType(), True),
        ])
        # here expected out put Data
        expected_data = [
            ("Washing Machine", "1648770933000",20000, "Samsung", "india", "001", "2022-04-01 05:25:33",
             "04-01-2022"),
            ("Refrigerator", "1648770999000",35000, " LG", "null", "002", "2022-04-01 05:26:39", "04-01-2022"),
            ("Air Cooler", "1648770948000",45000, "  Voltas", "null", "003", "2022-04-01 05:25:48", "04-01-2022")]
        # here CONVERT given schema and data to crate dataframe
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        # Apply transforamtion on the input data frame
        transformed_df = dateConvert(input_df)
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertFalse(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

        # Unit Test Case Remove the starting extra space in Brand column for LG and Voltas fields
        # here expected out put Schema
        expected_schema = StructType([

            StructField("Product_Name ", StringType(), True),
            StructField("IssueDate", StringType(), True),
            StructField("Price", LongType(), True),
            StructField("Brand", StringType(), True),
            StructField("Country", StringType(), True),
            StructField("Product_number  ", StringType(), True),
            StructField("NewBrand", StringType(), True)

        ])


        expected_schema = StructType([

            StructField("Product_Name ", StringType(), True),
            StructField("IssueDate", StringType(), True),
            StructField("Price", LongType(), True),
            StructField("Brand ", StringType(), True),
            StructField("user_Country", StringType(), True),
            StructField("Product_number  ", StringType(), True),
            StructField("Country", StringType(), True)])

        # here expected out put data
        expected_data = [
            ("Washing Machine", "1648770933000",20000, "Samsung", "india", "001","india"),
            ("Refrigerator", "1648770999000",35000, " LG", "null", "002",""),
            ("Air Cooler", "1648770948000",45000, "  Voltas", "null", "003","")]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        # Apply transforamtion on the input data frame
        transformed_df = removeNull(input_df)
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertFalse(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

    # Unit Test Case for add to column as start_time_ms and convert the values of StartTime to milliseconds.
        # here write input Schema
        input_Schema = StructType([
            StructField("SourceId ", IntegerType(), True),
            StructField("TransactionNumber", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("ModelNumber", StringType(), True),
            StructField("StartTime", StringType(), True),
            StructField("ProductNumber  ", StringType(), True)
        ])
        # here write input Data

        input_data = [(150711, 123456, "EN", 456789, "2022-04-01 05:25:33", "0001"),
                      (150439, 234567, "UK", 345678, "2022-04-01 05:26:39", "0002"),
                      (150647, 345678, "ES", 234567, "2022-04-01 05:25:48", "0003")]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_Schema)
        # here write expected out put Schema
        expected_schema = StructType([
            StructField("SourceId ", IntegerType(), True),
            StructField("TransactionNumber", IntegerType(), True),
            StructField("country", StringType(), True),
            StructField("ModelNumber", StringType(), True),
            StructField("StartTime", StringType(), True),
            StructField("ProductNumber  ", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("millisecond  ", StringType(), True)
        ])
        # here write expected out put data

        expected_data = [(150711, 123456, "EN", 456789, "2022-04-01 05:25:33", "1","2022, 4, 1, 5, 26, 39","1648770933"),
                      (150439, 234567, "UK", 345678, "2022-04-01 05:26:39", "2","2022, 4, 1, 5, 25, 48","1648770999"),
                      (150647, 345678, "ES", 234567, "2022-04-01 05:25:48", "3","2022, 4, 1, 5, 25, 33","1648770948")]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        # Apply transforamtion on the input data frame
        transformed_df = converyMilliSec(input_df)
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        #
        #  Unit Test Case for Combine both the tables based on the Product Number and get all the fields in return.
        input_Schema_User = StructType([
            StructField("User_name", StringType(), True),
            StructField("Productnumber", IntegerType(), True)
        ])

        input_data_user = [("arun",1001),("arul",1002),("raj",1003),("alwin",1004)]
        input_df_user = self.spark.createDataFrame(data=input_data_user, schema=input_Schema_User)

        input_Schema_transaction = StructType([
            StructField("Productnumber", IntegerType(), True),
            StructField("product", StringType(), True),
            StructField("country",StringType(),True)
        ])

        input_data_transaction = [(1001,"laptop","EN"), (1002,"mouse","UK"), (1003,"keyboard","US"), (1004,"harddisk","ES")]

        input_df_transation = self.spark.createDataFrame(data=input_data_transaction, schema=input_Schema_transaction)

        expected_schema = StructType([
            StructField("User_name", StringType(), True),
            StructField("Productnumber", IntegerType(), True),
            StructField("Productnumber", IntegerType(), True),
            StructField("product", StringType(), True),
            StructField("country", StringType(), True),
        ])
        expected_data = [("arun",1001,1001,"laptop","EN"), ("arul",1002,1002,"mouse","UK"), ("raj",1003,1003,"keyboard","US"), ("alwin",1004,1004,"harddisk","ES")]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)


        # Apply transforamtion on the input data frame
        transformed_df = joinDF(input_df_user,input_df_transation)
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertTrue(res)

        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

        # Unit test case for get the country as EN

        expected_schema = StructType([
            StructField("User_name", StringType(), True),
            StructField("Productnumber", IntegerType(), True),
            StructField("Productnumber", IntegerType(), True),
            StructField("product", StringType(), True),
            StructField("country", StringType(), True),
        ])
        expected_data = [("arun", 1001, 1001, "laptop", "EN")]

        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)
        # Apply transforamtion on the input data frame
        transformed_df = filterBYEN(transformed_df,"country")
        transformed_df.show()
        # Compare schema of transformed_df and expected_df
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        res = set(fields1) == set(fields2)
        print(fields1)
        print(fields2)
        # assert
        self.assertTrue(res)
        # Compare data in transformed_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))

if __name__ == '__main__':
    unittest.main()