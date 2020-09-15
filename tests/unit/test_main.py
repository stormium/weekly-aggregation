import unittest
import logging
from pyspark.sql import SparkSession

class PySparkTest(unittest.TestCase):
    def setUp(self):
        """
        Start Spark, define config and path to test data
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

    def test_default(self):
        self.assertEqual(50,51,
            'incorrect')
