import csv

from pyspark.ml import PipelineModel, Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler

mysql_url = "jdbc:mysql://localhost:3306/ebay?serverTimezone=UTC&useUnicode=true&zeroDateTimeBehavior" \
            "=convertToNull&autoReconnect=true&characterEncoding=utf-8"

prop = {'user': 'root', 'password': 'root', 'driver': "com.mysql.jdbc.Driver"}

path_regression_logistic = "data/model/quantity_sold/regression_logistic"
path_regression_random_forest = "data/model/quantity_sold/regression_random_forest"
path_regression_decision_tree = "data/model/quantity_sold/regression_decision_tree"

input_clos = [
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
    evaluator_accuracy = MulticlassClassificationEvaluator(labelCol='QuantitySold', metricName='accuracy') \
        .evaluate(predict_df)
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol='QuantitySold', metricName='f1') \
        .evaluate(predict_df)
    evaluator_recall = MulticlassClassificationEvaluator(labelCol="QuantitySold", metricName="weightedRecall") \
        .evaluate(predict_df)
    temp_list.append(evaluator_accuracy)
    temp_list.append(evaluator_f1)
    temp_list.append(evaluator_recall)
    return temp_list


# 保存逻辑回归模型
def write_regression_logistic(train_df, path):
    # 1.预处理训练数据
    assembler_train_df = get_assembler_train_df(train_df)
    # 2.创建逻辑回归
    regression = LogisticRegression() \
        .setLabelCol("QuantitySold") \
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
    regression = RandomForestClassifier() \
        .setSeed(10) \
        .setLabelCol("QuantitySold") \
        .setFeaturesCol("loan_feature") \
        .setPredictionCol("prediction") \
        .setNumTrees(10)
    # 3.添加到管道当中
    pipeline = Pipeline(stages=[regression])
    # 4.训练决策树模型
    model = pipeline.fit(assembler_train_df)
    # 5.保存模型
    model.write().overwrite().save(path)


# 保存决策树模型
def write_regression_decision_tree(train_df, path):
    # 1.预处理训练数据
    assembler_train_df = get_assembler_train_df(train_df)
    # 2.创建决策树
    regression = DecisionTreeClassifier() \
        .setSeed(10) \
        .setLabelCol("QuantitySold") \
        .setFeaturesCol("loan_feature") \
        .setPredictionCol("prediction")
    # 3.添加到管道当中
    pipeline = Pipeline(stages=[regression])
    # 4.训练决策树模型
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
        .drop("loan_feature", "QuantitySold", "rawPrediction", "probability") \
        .withColumnRenamed("prediction", "QuantitySold")\
        .write \
        .jdbc(mysql_url, table_name, 'overwrite', prop)


# 保存模型
def write_model(train_df):
    # 1.逻辑回归
    write_regression_logistic(train_df, path_regression_logistic)
    # 2.随机森林
    write_regression_random_forest(train_df, path_regression_random_forest)
    # 3.决策树
    write_regression_decision_tree(train_df, path_regression_decision_tree)


# 加载模型进行训练
def transform_model(predict_df):
    # 1.打开文件，追加a
    out = open(r"data/indicators/indicators_quantity_sold.csv", 'w', newline='', encoding='utf-8')
    # 2.设置写入模式
    csv_write = csv.writer(out, dialect='excel')
    # 3.写入头行
    csv_write.writerow(['id', 'algorithm_name', 'accurate_rate', 'recall_rate', 'f1_score'])
    # 4.逻辑回归
    load_model_transform(predict_df, path_regression_logistic, [1, "逻辑回归"], csv_write)
    # 5.随机森林
    load_model_transform(predict_df, path_regression_random_forest, [2, "随机森林"], csv_write)
    # 6.决策树
    load_model_transform(predict_df, path_regression_decision_tree, [3, "决策树"], csv_write)


# 加载模型进行训练
def transform_model_stream_quantity_sold(predict_df):
    # 4.逻辑回归
    load_model_transform_stream(predict_df, path_regression_logistic, "quantity_sold_regression_logistic")
    # 5.随机森林
    load_model_transform_stream(predict_df, path_regression_random_forest, "quantity_sold_regression_random_forest")
    # 6.决策树
    load_model_transform_stream(predict_df, path_regression_decision_tree, "quantity_sold_regression_decision_treer")


# 预测拍卖是否成功
def start_predict_quantity_sold(train_df, predict_df):
    # 1.保存模型
    write_model(train_df)
    # 2.加载模型进行训练
    transform_model(predict_df)
