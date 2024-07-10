import csv

from pyspark.ml import PipelineModel, Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.regression import LinearRegression
from pyspark.ml.regression import RandomForestRegressor

mysql_url = "jdbc:mysql://localhost:3306/ebay?serverTimezone=UTC&useUnicode=true&zeroDateTimeBehavior" \
            "=convertToNull&autoReconnect=true&characterEncoding=utf-8"

prop = {'user': 'root', 'password': 'root', 'driver': "com.mysql.jdbc.Driver"}

path_regression_linear = "data/model/price/regression_linear"
path_regression_random_forest = "data/model/price/regression_random_forest"
path_regression_gbt = "data/model/price/regression_gbt"

input_clos = [
    "QuantitySold",
    "PricePercent",
    "StartingBidPercent",
    "SellerClosePercent",
    "Category",
    "PersonID",
    "StartingBid",
    "AvgPrice",
    "HitCount",
    "AuctionAvgHitCount",
    "ItemAuctionSellPercent",
    "SellerSaleAvgPriceRatio",
    "SellerAvg",
    "SellerItemAvg",
    "AuctionHitCountAvgRatio",
    "BestOffer",
    "ReturnsAccepted",
    "IsHOF",
    "AuctionCount",
    "AuctionSaleCount",
    "SellerAuctionCount",
    "SellerAuctionSaleCount",
    "AuctionMedianPrice"
]


def get_assembler_train_df(train_df):
    assembler_train = VectorAssembler(inputCols=input_clos, outputCol="loan_feature")
    return assembler_train.transform(train_df).persist()


def get_assembler_predict_df(predict_df):
    assembler_predict = VectorAssembler(inputCols=input_clos, outputCol="loan_feature")
    return assembler_predict.transform(predict_df)


def get_regression_evaluator(predict_df, temp_list):
    evaluator_rmse = RegressionEvaluator(predictionCol="prediction", labelCol="Price", metricName="rmse")
    evaluator_mse = RegressionEvaluator(predictionCol="prediction", labelCol="Price", metricName="mse")
    evaluator_r2 = RegressionEvaluator(predictionCol="prediction", labelCol="Price", metricName="r2")
    evaluator_mae = RegressionEvaluator(predictionCol="prediction", labelCol="Price", metricName="mae")
    temp_list.append(evaluator_rmse.evaluate(predict_df))
    temp_list.append(evaluator_mse.evaluate(predict_df))
    temp_list.append(evaluator_r2.evaluate(predict_df))
    temp_list.append(evaluator_mae.evaluate(predict_df))
    return temp_list


# 保存线性回归模型
def write_regression_linear(train_df, path):
    # 1.预处理训练数据
    assembler_train_df = get_assembler_train_df(train_df)
    # 2.创建线性回归
    regression = LinearRegression() \
        .setLabelCol("Price") \
        .setFeaturesCol("loan_feature") \
        .setPredictionCol("prediction")
    # 3.添加到管道当中
    pipeline = Pipeline(stages=[regression])
    # 4.训练决策树模型
    model = pipeline.fit(assembler_train_df)
    # 5.保存模型
    model.write().overwrite().save(path)


# 保存随机森林模型
def write_regression_random_forest(train_df, path):
    # 1.预处理训练数据
    assembler_train_df = get_assembler_train_df(train_df)
    # 2.创建随机森林
    regression = RandomForestRegressor() \
        .setSeed(10) \
        .setLabelCol("Price") \
        .setFeaturesCol("loan_feature") \
        .setPredictionCol("prediction") \
        .setNumTrees(10)
    # 3.添加到管道当中
    pipeline = Pipeline(stages=[regression])
    # 4.训练决策树模型
    model = pipeline.fit(assembler_train_df)
    # 5.保存模型
    model.write().overwrite().save(path)


# 梯度下降树
def write_regression_gbt(train_df, path):
    # 1.预处理训练数据
    assembler_train_df = get_assembler_train_df(train_df)
    # 2.创建梯度下降树模型
    regression = GBTRegressor() \
        .setLabelCol("Price") \
        .setFeaturesCol("loan_feature") \
        .setPredictionCol("prediction")
    # 3.添加到管道当中
    pipeline = Pipeline(stages=[regression])
    # 4.训练梯度下降树模型
    model = pipeline.fit(assembler_train_df)
    # 5.保存模型
    model.write().overwrite().save(path)


# 加载模型进行训练
def load_model_transform(predict_df, path, indicators_list, csv_write):
    # 1.加载模型
    model = PipelineModel.load(path)
    # 2.预处理测试数据
    assembler_predict_df = get_assembler_predict_df(predict_df)
    # 3.进行决策树预测
    predict_df = model.transform(assembler_predict_df)
    # 7.添加指标
    data_list = get_regression_evaluator(predict_df, indicators_list)
    # 8.写入数据
    csv_write.writerow(data_list)


# 加载模型进行训练
def load_model_transform_stream(predict_df, path, table_name):
    # 1.加载模型
    model = PipelineModel.load(path)
    # 2.预处理测试数据
    assembler_predict_df = get_assembler_predict_df(predict_df)
    # 3.进行决策树预测
    model \
        .transform(assembler_predict_df) \
        .drop("loan_feature", "Price") \
        .withColumnRenamed("prediction", "Price") \
        .write \
        .jdbc(mysql_url, table_name, 'overwrite', prop)


# 保存模型
def write_model(train_df):
    # 1.线性回归
    write_regression_linear(train_df, path_regression_linear)
    # 2.随机森林
    write_regression_random_forest(train_df, path_regression_random_forest)
    # 3.梯度下降树
    write_regression_gbt(train_df, path_regression_gbt)


# 加载模型进行训练
def transform_model(predict_df):
    # 1.打开文件，追加a
    out = open(r"data/indicators/indicators_price.csv", 'w', newline='', encoding='utf-8')
    # 2.设置写入模式
    csv_write = csv.writer(out, dialect='excel')
    # 3.写入头行
    csv_write.writerow(['id', 'algorithm_name', 'rmse', 'mse', 'r2', 'mae'])
    # 4.线性回归
    load_model_transform(predict_df, path_regression_linear, [1, "线性回归"], csv_write)
    # 5.随机森林
    load_model_transform(predict_df, path_regression_random_forest, [2, "随机森林"], csv_write)
    # 6.梯度下降树
    load_model_transform(predict_df, path_regression_gbt, [3, "梯度下降树"], csv_write)


# 加载模型进行训练
def transform_model_stream_price(predict_df):
    # 1.线性回归
    load_model_transform_stream(predict_df, path_regression_linear, "price_regression_linear")
    # 2.随机森林
    load_model_transform_stream(predict_df, path_regression_random_forest, "price_regression_random_forest")
    # 3.梯度下降树
    load_model_transform_stream(predict_df, path_regression_gbt, "price_regression_gbt")


# 预测价格
def start_predict_price(train_df, predict_df):
    # 1.保存模型
    write_model(train_df)
    # 2.加载模型进行训练
    transform_model(predict_df)
