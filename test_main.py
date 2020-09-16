import unittest
import logging
from pyspark.sql import SparkSession

class PySparkTest(unittest.TestCase):
    def setUp(self):
        """
        Start Spark
        """
        self.spark=SparkSession.builder \
            .appName("MyApp") \
            .config("spark.sql.shuffle.partitions","2") \
            .getOrCreate()

    def tearDown(self):
        """
        Stop Spark
        """
        self.spark.stop()

    def test_add_week_partition(self):
        import pandas as pd
        from main import step_adds_week_partition
        pandas_data = pd.DataFrame(
            {
                'order_date': ['1/2/2019','1/5/2019','1/5/2019','1/6/2019','1/7/2019']
            }
        )
        pandas_data['order_date']= pd.to_datetime(pandas_data['order_date'])
        sdf = self.spark.createDataFrame(pandas_data)
        results = step_adds_week_partition(sdf)
        expected_results = pd.DataFrame(
            {
                'order_date': ['1/2/2019','1/5/2019','1/5/2019','1/6/2019','1/7/2019'],
                'dt': ['20181231','20181231','20181231','20181231','20190107']
            }
        )
        expected_results['order_date']= pd.to_datetime(expected_results['order_date'])
        data_spark_expexted = self.spark.createDataFrame(expected_results)

        self.assertEqual(results,data_spark_expexted,
            'incorrect')
