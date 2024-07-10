import time

import findspark

findspark.init()
from pyspark.sql import SparkSession
from spark_predict_price import transform_model_stream_price
from spark_predict_quantity_sold import transform_model_stream_quantity_sold

mysql_url = "jdbc:mysql://localhost:3306/ebay?serverTimezone=UTC&useUnicode=true&zeroDateTimeBehavior" \
            "=convertToNull&autoReconnect=true&characterEncoding=utf-8"

prop = {'user': 'root', 'password': 'root', 'driver': "com.mysql.jdbc.Driver"}

sql = """
    SELECT
        CAST(EbayID AS BIGINT)                      AS EbayID
        ,CAST(Price AS DOUBLE)                      AS Price
        ,CAST(PricePercent AS DOUBLE)               AS PricePercent
        ,CAST(StartingBidPercent AS DOUBLE)         AS StartingBidPercent
        ,SellerName                                 AS SellerName
        ,CAST(SellerClosePercent AS DOUBLE)         AS SellerClosePercent
        ,CAST(Category AS INT)                      AS Category
        ,CAST(PersonID AS INT)                      AS PersonID
        ,CAST(StartingBid AS DOUBLE)                AS StartingBid
        ,CAST(AvgPrice AS DOUBLE)                   AS AvgPrice
        ,EndDay                                     AS EndDay
        ,CAST(QuantitySold AS DOUBLE)               AS QuantitySold
        ,CAST(HitCount AS INT)                      AS HitCount
        ,CAST(AuctionAvgHitCount AS INT)            AS AuctionAvgHitCount
        ,CAST(ItemAuctionSellPercent AS DOUBLE)     AS ItemAuctionSellPercent
        ,CAST(SellerSaleAvgPriceRatio AS DOUBLE)    AS SellerSaleAvgPriceRatio
        ,CAST(SellerAvg AS DOUBLE)                  AS SellerAvg
        ,CAST(SellerItemAvg AS DOUBLE)              AS SellerItemAvg
        ,CAST(AuctionHitCountAvgRatio AS DOUBLE)    AS AuctionHitCountAvgRatio
        ,CAST(BestOffer AS DOUBLE)                  AS BestOffer
        ,CAST(ReturnsAccepted AS INT)               AS ReturnsAccepted
        ,CAST(IsHOF AS INT)                         AS IsHOF
        ,CAST(BidCount AS INT)                      AS BidCount
        ,CAST(AuctionCount AS INT)                  AS AuctionCount
        ,CAST(AuctionSaleCount AS INT)              AS AuctionSaleCount
        ,CAST(SellerAuctionCount AS INT)            AS SellerAuctionCount
        ,CAST(SellerAuctionSaleCount AS INT)        AS SellerAuctionSaleCount
        ,CAST(PriceBuckets AS INT)                  AS PriceBuckets
        ,CAST(AuctionMedianPrice AS DOUBLE)         AS AuctionMedianPrice
        ,CAST(IsInMedianRatio10Percent AS INT)      AS IsInMedianRatio10Percent
        ,CAST(IsInMedianRatio20Percent AS INT)      AS IsInMedianRatio20Percent
        ,CAST(IsInMedianRatio25Percent AS INT)      AS IsInMedianRatio25Percent
    FROM
        stream
"""


def start_task():
    # 1.创建SparkSession
    spark = SparkSession.builder.appName("spark_etl").getOrCreate()
    # 2.读取本地数据集
    dataframe_training = spark.read.csv('hdfs://192.168.78.128:9000/ebay/stream/*.csv') \
        .withColumnRenamed("_c0", "EbayID") \
        .withColumnRenamed("_c1", "Price") \
        .withColumnRenamed("_c2", "PricePercent") \
        .withColumnRenamed("_c3", "StartingBidPercent") \
        .withColumnRenamed("_c4", "SellerName") \
        .withColumnRenamed("_c5", "SellerClosePercent") \
        .withColumnRenamed("_c6", "Category") \
        .withColumnRenamed("_c7", "PersonID") \
        .withColumnRenamed("_c8", "StartingBid") \
        .withColumnRenamed("_c9", "AvgPrice") \
        .withColumnRenamed("_c10", "EndDay") \
        .withColumnRenamed("_c11", "QuantitySold") \
        .withColumnRenamed("_c12", "HitCount") \
        .withColumnRenamed("_c13", "AuctionAvgHitCount") \
        .withColumnRenamed("_c14", "ItemAuctionSellPercent") \
        .withColumnRenamed("_c15", "SellerSaleAvgPriceRatio") \
        .withColumnRenamed("_c16", "SellerAvg") \
        .withColumnRenamed("_c17", "SellerItemAvg") \
        .withColumnRenamed("_c18", "AuctionHitCountAvgRatio") \
        .withColumnRenamed("_c19", "BestOffer") \
        .withColumnRenamed("_c20", "ReturnsAccepted") \
        .withColumnRenamed("_c21", "IsHOF") \
        .withColumnRenamed("_c22", "BidCount") \
        .withColumnRenamed("_c23", "AuctionCount") \
        .withColumnRenamed("_c24", "AuctionSaleCount") \
        .withColumnRenamed("_c25", "SellerAuctionCount") \
        .withColumnRenamed("_c26", "SellerAuctionSaleCount") \
        .withColumnRenamed("_c27", "PriceBuckets") \
        .withColumnRenamed("_c28", "AuctionMedianPrice") \
        .withColumnRenamed("_c29", "IsInMedianRatio10Percent") \
        .withColumnRenamed("_c30", "IsInMedianRatio20Percent") \
        .withColumnRenamed("_c31", "IsInMedianRatio25Percent")
    # 3.注册表
    dataframe_training.createOrReplaceTempView("stream")
    # 4.获取数据
    predict_df = spark.sql(sql)
    # 5.预测价格
    transform_model_stream_price(predict_df)
    # 6.预测是否拍卖成功
    transform_model_stream_quantity_sold(predict_df)
    # 7.释放资源
    spark.stop()


if __name__ == '__main__':
    while True:
        start_task()
        time.sleep(30)
