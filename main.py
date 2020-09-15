import pandas
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

#init spark
def init_spark():
  spark = SparkSession.builder.appName("WeeklyAggregation").getOrCreate()
  sc = spark.sparkContext
  return spark,sc

#helper f-ions
def step_adds_week_partition(input):
    output = input \
        .withColumn('dt', date_trunc('week', 'order_date')) \
        .withColumn('dt', date_format('dt', 'yyyyMMdd'))
    return output

def step_look_for_delivery_fee_col(df): #adds empty delivery fee col into input data, if not available in sample data xlsx
    if 'order_delivery_fee' not in df.columns:
        df['order_delivery_fee'] = 0
    return df

def main():
    spark,sc = init_spark()

    excel_data_df = pandas.read_excel('input_data.xlsx', sheet_name='orders_daily_sample')
    df = step_look_for_delivery_fee_col(excel_data_df)
    sdf = spark.createDataFrame(df)
    # sdf.show()
    sdf1 = step_adds_week_partition(sdf)

    #main aggregation goes here
    sdf2 = sdf1 \
    .groupBy('customer_id','dt') \
    .agg(
        ###- order_executed_count -###
        count(
            when(col('order_type') == 'Delivered', 1)
            .when(col('order_type') == 'Collected', 1)
        )
        .alias('order_executed_count'),

        ###- order_placed_count -###
        count(
            when(col('order_type') == 'Placed', 1)
        )
        .alias('order_placed_count'),

        ###- order_returned_count -###
        count(
            when(col('order_type') == 'Returned', 1)
        )
        .alias('order_returned_count'),

        round(
            avg(
                col('order_size')
            ), 0
        )
        .cast("int")
        .alias('order_size_avg'),

        sum(
            when(col('order_type') == 'Delivered', col('order_size'))
            .when(col('order_type') == 'Collected', col('order_size'))
            .otherwise(0)
        )
        .alias('order_size_executed_sum'),

        sum(
            when(col('order_type') == 'Placed', col('order_size'))
            .otherwise(0)
        )
        .alias('order_size_placed_sum'),

        sum(
            when(col('order_type') == 'Returned', col('order_size'))
            .otherwise(0)
        )
        .alias('order_size_returned_sum'),


        sum(
            col('order_revenue')
        )
        .cast(DecimalType(19, 4))
        .alias('order_revenue_sum'),

        avg(
            col('order_revenue')
        )
        .cast(DecimalType(19, 4))
        .alias('order_revenue_avg'),

        avg(
            when(col('order_type') == 'Delivered', col('order_revenue'))
            .when(col('order_type') == 'Collected', col('order_revenue'))
            .otherwise(0)
        )
        .cast(DecimalType(19, 4))
        .alias('order_revenue_executed_avg'),

        avg(
            when(col('order_type') == 'Placed', col('order_revenue'))
            .otherwise(0)
        )
        .cast(DecimalType(19, 4))
        .alias('order_revenue_placed_avg'),

        avg(
            when(col('order_type') == 'Returned', col('order_revenue'))
            .otherwise(0)
        )
        .cast(DecimalType(19, 4))
        .alias('order_revenue_returned_avg'),

        sum(
            col('order_profit')
        )
        .cast(DecimalType(19, 4))
        .alias('order_profit_sum'),

        avg(
            col('order_profit')
        )
        .cast(DecimalType(19, 4))
        .alias('order_profit_avg'),

        avg(
            when(col('order_type') == 'Delivered', col('order_profit'))
            .when(col('order_type') == 'Collected', col('order_profit'))
            .otherwise(0)
        )
        .cast(DecimalType(19, 4))
        .alias('order_profit_executed_avg'),

        avg(
            when(col('order_type') == 'Placed', col('order_profit'))
            .otherwise(0)
        )
        .cast(DecimalType(19, 4))
        .alias('order_profit_placed_avg'),

        avg(
            when(col('order_type') == 'Returned', col('order_profit'))
            .otherwise(0)
        )
        .cast(DecimalType(19, 4))
        .alias('order_profit_returned_avg'),

        max(
            col('discount_active')
        )
        .alias('discount_active'),

        avg(
            col('discount_amount')
        )
        .cast(DecimalType(19, 4))
        .alias('discount_amount_avg'),

        ###- last_address -### TBD
        ###- top_address -### TBD
        ###- payment_source_top -### TBD

        ###- delivery_fee_avg -###
        avg(
            col('order_delivery_fee')
        )
        .cast(DecimalType(19, 4))
        .alias('delivery_fee_avg'),

        ###- order_count_w_delivery_fee -###
        count(
            when(col('order_delivery_fee') > 0, 1)
            .otherwise(None)
        )
        .alias('order_count_w_delivery_fee'),

        ###- packaging_fee_avg -###
        avg(
            col('order_packaging_fee')
        )
        .cast(DecimalType(19, 4))
        .alias('packaging_fee_avg'),

        ###- order_count_w_delivery_fee -###
        count(
            when(col('order_packaging_fee') > 0, 1)
            .otherwise(None)
        )
        .alias('order_count_w_packaging_fee')
    )

    sdf2.show()

    sdf2.toPandas().to_excel('output1.xlsx', engine='xlsxwriter')

if __name__ == '__main__':
  main()
