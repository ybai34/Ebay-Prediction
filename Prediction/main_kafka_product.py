import json
import time
import traceback

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import kafka_errors

producer = KafkaProducer(
    bootstrap_servers=['192.168.78.128:9092', '192.168.78.129:9092', '192.168.78.130:9092'],
    key_serializer=lambda k: json.dumps(k).encode(),
    value_serializer=lambda v: json.dumps(v).encode())


def send_kafka(data):
    send_data = producer.send(topic='ebay', key="key", value=data)
    try:
        send_data.get(timeout=10)
    except kafka_errors:
        traceback.format_exc()


def send_test_subset():
    # 1.读取数据
    TestSubset = pd.read_csv('data/source/TestSubset.csv')
    # 2.遍历数据
    for index in range(len(TestSubset)):
        # 3.构建数据
        data = {
            'EbayID': str(TestSubset['EbayID'][index]),
            'Price': str(TestSubset['Price'][index]),
            'PricePercent': str(TestSubset['PricePercent'][index]),
            'StartingBidPercent': str(TestSubset['StartingBidPercent'][index]),
            'SellerName': str(TestSubset['SellerName'][index]),
            'SellerClosePercent': str(TestSubset['SellerClosePercent'][index]),
            'Category': str(TestSubset['Category'][index]),
            'PersonID': str(TestSubset['PersonID'][index]),
            'StartingBid': str(TestSubset['StartingBid'][index]),
            'AvgPrice': str(TestSubset['AvgPrice'][index]),
            'EndDay': str(TestSubset['EndDay'][index]),
            'QuantitySold': str(TestSubset['QuantitySold'][index]),
            'HitCount': str(TestSubset['HitCount'][index]),
            'AuctionAvgHitCount': str(TestSubset['AuctionAvgHitCount'][index]),
            'Authenticated': str(TestSubset['Authenticated'][index]),
            'ItemAuctionSellPercent': str(TestSubset['ItemAuctionSellPercent'][index]),
            'SellerSaleAvgPriceRatio': str(TestSubset['SellerSaleAvgPriceRatio'][index]),
            'SellerAvg': str(TestSubset['SellerAvg'][index]),
            'SellerItemAvg': str(TestSubset['SellerItemAvg'][index]),
            'AuctionHitCountAvgRatio': str(TestSubset['AuctionHitCountAvgRatio'][index]),
            'BestOffer': str(TestSubset['BestOffer'][index]),
            'ReturnsAccepted': str(TestSubset['ReturnsAccepted'][index]),
            'IsHOF': str(TestSubset['IsHOF'][index]),
            'BidCount': str(TestSubset['BidCount'][index]),
            'AuctionCount': str(TestSubset['AuctionCount'][index]),
            'AuctionSaleCount': str(TestSubset['AuctionSaleCount'][index]),
            'SellerAuctionCount': str(TestSubset['SellerAuctionCount'][index]),
            'SellerAuctionSaleCount': str(TestSubset['SellerAuctionSaleCount'][index]),
            'PriceBuckets': str(TestSubset['PriceBuckets'][index]),
            'AuctionMedianPrice': str(TestSubset['AuctionMedianPrice'][index]),
            'IsInMedianRatio10Percent': str(TestSubset['IsInMedianRatio10Percent'][index]),
            'IsInMedianRatio20Percent': str(TestSubset['IsInMedianRatio20Percent'][index]),
            'IsInMedianRatio25Percent': str(TestSubset['IsInMedianRatio25Percent'][index])
        }
        # 4.输出数据
        print(data)
        # 5.设置延迟时间
        time.sleep(10)
        # 6.发送到Kafka
        send_kafka(data)


if __name__ == '__main__':
    send_test_subset()
