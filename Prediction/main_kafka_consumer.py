import json

import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext, Row
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sql = """
    SELECT
        EbayID
        ,Price
        ,PricePercent
        ,StartingBidPercent
        ,SellerName
        ,SellerClosePercent
        ,Category
        ,PersonID
        ,StartingBid
        ,AvgPrice
        ,EndDay
        ,QuantitySold
        ,HitCount
        ,AuctionAvgHitCount
        ,ItemAuctionSellPercent
        ,SellerSaleAvgPriceRatio
        ,SellerAvg
        ,SellerItemAvg
        ,AuctionHitCountAvgRatio
        ,BestOffer
        ,ReturnsAccepted
        ,IsHOF
        ,BidCount
        ,AuctionCount
        ,AuctionSaleCount
        ,SellerAuctionCount
        ,SellerAuctionSaleCount
        ,PriceBuckets
        ,AuctionMedianPrice
        ,IsInMedianRatio10Percent
        ,IsInMedianRatio20Percent
        ,IsInMedianRatio25Percent
    FROM
        stream
"""


def getSparkSessionInstance(spark_conf):
    if 'sparkSessionSingletonInstance' not in globals():
        globals()['sparkSessionSingletonInstance'] = SparkSession.builder.config(conf=spark_conf).getOrCreate()
    return globals()['sparkSessionSingletonInstance']


def getRow(str_json):
    return Row(
        EbayID=str_json.get("EbayID"),
        Price=str_json.get("Price"),
        PricePercent=str_json.get("PricePercent"),
        StartingBidPercent=str_json.get("StartingBidPercent"),
        SellerName=str_json.get("SellerName"),
        SellerClosePercent=str_json.get("SellerClosePercent"),
        Category=str_json.get("Category"),
        PersonID=str_json.get("PersonID"),
        StartingBid=str_json.get("StartingBid"),
        AvgPrice=str_json.get("AvgPrice"),
        EndDay=str_json.get("EndDay"),
        QuantitySold=str_json.get("QuantitySold"),
        HitCount=str_json.get("HitCount"),
        AuctionAvgHitCount=str_json.get("AuctionAvgHitCount"),
        ItemAuctionSellPercent=str_json.get("ItemAuctionSellPercent"),
        SellerSaleAvgPriceRatio=str_json.get("SellerSaleAvgPriceRatio"),
        SellerAvg=str_json.get("SellerAvg"),
        SellerItemAvg=str_json.get("SellerItemAvg"),
        AuctionHitCountAvgRatio=str_json.get("AuctionHitCountAvgRatio"),
        BestOffer=str_json.get("BestOffer"),
        ReturnsAccepted=str_json.get("ReturnsAccepted"),
        IsHOF=str_json.get("IsHOF"),
        BidCount=str_json.get("BidCount"),
        AuctionCount=str_json.get("AuctionCount"),
        AuctionSaleCount=str_json.get("AuctionSaleCount"),
        SellerAuctionCount=str_json.get("SellerAuctionCount"),
        SellerAuctionSaleCount=str_json.get("SellerAuctionSaleCount"),
        PriceBuckets=str_json.get("PriceBuckets"),
        AuctionMedianPrice=str_json.get("AuctionMedianPrice"),
        IsInMedianRatio10Percent=str_json.get("IsInMedianRatio10Percent"),
        IsInMedianRatio20Percent=str_json.get("IsInMedianRatio20Percent"),
        IsInMedianRatio25Percent=str_json.get("IsInMedianRatio25Percent")
    )


def process(rdd):
    try:
        spark = getSparkSessionInstance(rdd.context.getConf())
        rowRdd = rdd.map(lambda x: json.loads(x)).map(lambda str_json: getRow(str_json))
        spark.createDataFrame(rowRdd).createOrReplaceTempView("stream")
        spark.sql(sql).write.format("csv").mode("append").save("hdfs://192.168.78.128:9000/ebay/stream")
    except:
        pass


if __name__ == '__main__':
    # 1.创建SparkContext
    spark_context = SparkContext(appName="streaming_kafka")
    # 2.创建StreamingContext
    spark_stream_context = StreamingContext(spark_context, 10)
    # 3.读取Kafka
    kafka_streaming_rdd = KafkaUtils.createDirectStream(
        spark_stream_context,
        ["ebay"],
        {
            "metadata.broker.list": "192.168.78.128:9092",
            "group.id": "test2",
            "fetch.message.max.bytes": "15728640"
        }
    )
    # 4.获取数据
    words = kafka_streaming_rdd.map(lambda x: x[1])
    words.foreachRDD(process)
    # 5.开始任务
    spark_stream_context.start()
    spark_stream_context.awaitTermination()
