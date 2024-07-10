import findspark

findspark.init()
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from spark_predict_price import start_predict_price
from spark_predict_quantity_sold import start_predict_quantity_sold

mysql_url = "jdbc:mysql://localhost:3306/ebay?serverTimezone=UTC&useUnicode=true&zeroDateTimeBehavior" \
            "=convertToNull&autoReconnect=true&characterEncoding=utf-8"

prop = {'user': 'root', 'password': 'root', 'driver': "com.mysql.jdbc.Driver"}

hdfs_training = "hdfs://192.168.78.128:9000/ebay/train"
hdfs_predict = "hdfs://192.168.78.128:9000/ebay/predict"

schema_test = StructType([
    StructField("EbayID", LongType(), True),
    StructField("QuantitySold", DoubleType(), True),
    StructField("Price", DoubleType(), True),
    StructField("PricePercent", DoubleType(), True),
    StructField("StartingBidPercent", DoubleType(), True),
    StructField("SellerName", StringType(), True),
    StructField("SellerClosePercent", DoubleType(), True),
    StructField("Category", IntegerType(), True),
    StructField("PersonID", IntegerType(), True),
    StructField("StartingBid", DoubleType(), True),
    StructField("AvgPrice", DoubleType(), True),
    StructField("EndDay", StringType(), True),
    StructField("HitCount", IntegerType(), True),
    StructField("AuctionAvgHitCount", IntegerType(), True),
    StructField("ItemAuctionSellPercent", DoubleType(), True),
    StructField("SellerSaleAvgPriceRatio", DoubleType(), True),
    StructField("SellerAvg", DoubleType(), True),
    StructField("SellerItemAvg", IntegerType(), True),
    StructField("AuctionHitCountAvgRatio", IntegerType(), True),
    StructField("BestOffer", DoubleType(), True),
    StructField("ReturnsAccepted", IntegerType(), True),
    StructField("IsHOF", IntegerType(), True),
    StructField("ItemListedCount", IntegerType(), True),
    StructField("AuctionCount", IntegerType(), True),
    StructField("AuctionSaleCount", IntegerType(), True),
    StructField("SellerAuctionCount", IntegerType(), True),
    StructField("SellerAuctionSaleCount", IntegerType(), True),
    StructField("AuctionMedianPrice", DoubleType(), True)
])
schema_training = StructType([
    StructField("EbayID", LongType(), True),
    StructField("QuantitySold", DoubleType(), True),
    StructField("Price", DoubleType(), True),
    StructField("PricePercent", DoubleType(), True),
    StructField("StartingBidPercent", DoubleType(), True),
    StructField("SellerName", StringType(), True),
    StructField("SellerClosePercent", DoubleType(), True),
    StructField("Category", IntegerType(), True),
    StructField("PersonID", IntegerType(), True),
    StructField("StartingBid", DoubleType(), True),
    StructField("AvgPrice", DoubleType(), True),
    StructField("EndDay", StringType(), True),
    StructField("HitCount", IntegerType(), True),
    StructField("AuctionAvgHitCount", IntegerType(), True),
    StructField("ItemAuctionSellPercent", DoubleType(), True),
    StructField("SellerSaleAvgPriceRatio", DoubleType(), True),
    StructField("SellerAvg", DoubleType(), True),
    StructField("SellerItemAvg", IntegerType(), True),
    StructField("AuctionHitCountAvgRatio", IntegerType(), True),
    StructField("BestOffer", DoubleType(), True),
    StructField("ReturnsAccepted", IntegerType(), True),
    StructField("IsHOF", IntegerType(), True),
    StructField("ItemListedCount", IntegerType(), True),
    StructField("AuctionCount", IntegerType(), True),
    StructField("AuctionSaleCount", IntegerType(), True),
    StructField("SellerAuctionCount", IntegerType(), True),
    StructField("SellerAuctionSaleCount", IntegerType(), True),
    StructField("AuctionMedianPrice", DoubleType(), True)
])

schema_indicators_price = StructType([
    StructField("id", IntegerType(), True),
    StructField("algorithm_name", StringType(), True),
    StructField("rmse", DoubleType(), True),
    StructField("mse", DoubleType(), True),
    StructField("r2", DoubleType(), True),
    StructField("mae", DoubleType(), True)
])

schema_indicators_quantity_sold = StructType([
    StructField("id", IntegerType(), True),
    StructField("algorithm_name", StringType(), True),
    StructField("accurate_rate", DoubleType(), True),
    StructField("recall_rate", DoubleType(), True),
    StructField("f1_score", DoubleType(), True)
])


def etl_data_frame(dataframe):
    return dataframe \
        .where("Price IS NOT NULL") \
        .where("SellerName IS NOT NULL") \
        .where("EndDay IS NOT NULL") \
        .where("HitCount > 0") \
        .distinct()


if __name__ == '__main__':
    # 1.创建SparkSession
    spark = SparkSession.builder.appName("spark_etl").getOrCreate()
    # 2.读取本地数据集
    dataframe_training = spark.read.option("header", True).csv('data/source/TrainingSet.csv', schema=schema_training)
    dataframe_test = spark.read.option("header", True).csv('data/source/TestSet.csv', schema=schema_test)
    # 3.对数据进行清洗
    dataframe_training = etl_data_frame(dataframe_training)
    dataframe_test = etl_data_frame(dataframe_test)
    # 4.将清洗后的数据写入HDFS
    dataframe_training.write.mode("overwrite").options(header="true").csv(hdfs_training)
    dataframe_test.write.mode("overwrite").options(header="true").csv(hdfs_predict)
    # 5.读取HDFS的数据
    train_df = spark.read.option("header", True).csv(hdfs_training, schema=schema_training)
    predict_df = spark.read.option("header", True).csv(hdfs_predict, schema=schema_test)
    # 6.预测拍卖最终价格
    start_predict_price(train_df, predict_df)
    # 7.预测是否拍卖成功
    start_predict_quantity_sold(train_df, predict_df)
    # 8.读取算算法衡量指标数据写入Mysql
    spark.read.option("header", True) \
        .csv('data/indicators/indicators_price.csv', schema=schema_indicators_price) \
        .write.jdbc(mysql_url, 'indicators_price', 'overwrite', prop)
    spark.read.option("header", True) \
        .csv('data/indicators/indicators_quantity_sold.csv', schema=schema_indicators_quantity_sold) \
        .write.jdbc(mysql_url, 'indicators_quantity_sold', 'overwrite', prop)
    # 9.写入Mysql并注册表
    train_df.write.jdbc(mysql_url, 'training', 'overwrite', prop)
    train_df.createOrReplaceTempView("ebay")
    # 10.竞拍轮数与拍卖成功的关系
    task1_sql = '''
        SELECT
            range_name  AS range_name
            ,count(1)   AS counts
        FROM
            (
            SELECT
                CASE WHEN HitCount < 10 THEN '1-10'
                     WHEN HitCount < 20 THEN '10-20'
                     WHEN HitCount < 40 THEN '20-40'
                     WHEN HitCount < 100 THEN '40-100'
                     WHEN HitCount < 200 THEN '100-200'
                    WHEN HitCount < 300 THEN '200-300'
                    ELSE '>300' END AS range_name
            FROM
                ebay
            WHERE
                QuantitySold = 1.0
            ) AS temp1
        GROUP BY
            range_name
    '''
    spark.sql(task1_sql).write.jdbc(mysql_url, 'task_hit_count', 'overwrite', prop)
    # 11.星期与拍卖成功关系分析
    task2_sql = '''
        SELECT
            EndDay          AS weeks
            ,COUNT(1)       AS counts
        FROM
            ebay
        WHERE
            QuantitySold = 1.0
        GROUP BY
            EndDay
    '''
    spark.sql(task2_sql).write.jdbc(mysql_url, 'task_week', 'overwrite', prop)
    # 12.卖家活跃度分析
    task3_sql = """
        SELECT
            SellerName  AS seller_name
            ,count(1)   AS counts
        FROM
            ebay
        WHERE
            QuantitySold = 1.0
        GROUP BY
            SellerName
        ORDER BY
            counts DESC LIMIT 10
    """
    spark.sql(task3_sql).write.jdbc(mysql_url, 'task_seller_name', 'overwrite', prop)
    # 13.卖方好评率与拍卖成功关系
    task4_sql = '''
        SELECT
            range_name  AS range_name
            ,count(1)   AS counts
        FROM
            (
            SELECT
                CASE WHEN SellerSaleAvgPriceRatio < 0.2 THEN '0-0.2'
                     WHEN SellerSaleAvgPriceRatio < 0.4 THEN '0.2-0.4'
                     WHEN SellerSaleAvgPriceRatio < 0.6 THEN '0.4-0.6'
                     WHEN SellerSaleAvgPriceRatio < 0.8 THEN '0.6-0.8'
                     ELSE '0.8-1.0' END AS range_name
                FROM
                    ebay
                WHERE
                    QuantitySold = 1.0
            ) AS temp1
        GROUP BY
            range_name
    '''
    spark.sql(task4_sql).write.jdbc(mysql_url, 'task_ratio', 'overwrite', prop)
    # 14.价格与拍卖成功关系
    task5_sql = '''
        SELECT
            range_name  AS range_name
            ,count(1)   AS counts
        FROM
            (
            SELECT
                CASE WHEN Price < 2 THEN '0-2'
                     WHEN Price < 10 THEN '2-10'
                     WHEN Price < 20 THEN '10-20'
                     WHEN Price < 50 THEN '20-50'
                     WHEN Price < 120 THEN '50-120'
                     ELSE '>120' END AS range_name
                FROM
                    ebay
                WHERE
                    QuantitySold = 1.0
            ) AS temp1
        GROUP BY
            range_name
        '''
    spark.sql(task5_sql).write.jdbc(mysql_url, 'task_price', 'overwrite', prop)
