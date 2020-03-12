import os
import pandas as pd
import pyspark.sql.functions as F

from pyspark import SparkConf
from pyspark.sql import SparkSession


def get_spark_session(**kwargs):
    """Initializing Spark context.

    Parameters
    ----------
    kwargs : dict
        Variable number of keyword arguments to initialize the SparkContext
        object

    Returns
    -------
     : SparkContext object

    """
    conf = SparkConf() \
        .setAppName(kwargs.get('app_name', 'test')) \
        .set("spark.executor.memory", kwargs.get('executor_memory', '2g')) \
        .set("spark.executor.instances", kwargs.get('num_executors', '100')) \
        .set("spark.executor.cores", kwargs.get('num_cores', '2')) \
        .set("spark.yarn.queue", kwargs.get('spark_yarn_queue', 'ds-regular'))

    spark_session = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()

    return spark_session


def get_dataframe_from_csv(csv_file):
    """ get pandas dataframe from csv file """
    df = pd.read_csv(csv_file)
    df.dropna(inplace=True)
    return df


if __name__ == '__main__':
    ## initialize spark environment
    your_name = "libohang"  # put your name here
    spark_appname = "get_item_profile_{}".format(your_name)
    # spark conf parameter
    executor_memory = '10g'

    # get the spark object
    spark = get_spark_session(app_name=spark_appname,
                              executor_memory=executor_memory)
    sc = spark.sparkContext

    # this is mandatory in order for you to query from our DB
    spark.sql('use shopee')
    total_cores = int(sc._conf.get('spark.executor.instances')) * int(sc._conf.get('spark.executor.cores'))
    print("Num of cores:", total_cores)

    # getting data
    query = """
SELECT distinct(itemid),
level3_cat,
level3_category,
images
FROM 
item_profile
WHERE 
grass_region ='ID' and  level3_cat in (7271,7272,7273,7274,7275,12395,16621,7276,7277,7278,7279,12396,17858,7286,7287,7288,7289,7290,12400,12401,7291,7292,7293,7294,12397,12399,16624,16625,16626,7296,7297,7298,7300,12404,17859,12405,16627,7301,7303,7304,12411,7305,7306,7307,16883,7308,7309,7310,7311,7312,7313,12407,7314,7315,7316,12408,12409,7280,7281,7282,7283,7284,7285,16622,12414,12415,16976,12777,12778,12779,18197,18198,18199,18200,18202,18203,18204,18205,18207,18209,18210,18211,18212,18214,18215,18216,18217,18218,18220,18221,7546,18709,18710,7554,18689,18690,7559,7559,7560,7561,12592,18708,7562,7562,7563,12594,17898,18811,5472,5472,17897,12595,12595,12597,17900,17901,17902,17903,12780,18462,7544,7545,12789,17895,12794,18301,12803,18880,12808,12815,12816,12822,12823,12824,17896,5515,5514,7383,12598,5512,7391,7391,7392,7393,7394,7396,7404,7405,7406,7407,16973,16974,17884,5516,12607,12608,17880,16631,16632,17882,17883,18440,18442,12448,12449,12456,12457,12462,17878,12466,12467,12471,14966,14967,7317,7318,7319,7320,12426,7321,7323,12427,12430,12432,17868,17869,7326,17866,17867,7327,7328,7329,7330,17864,17865,7331,7332,12434,12435,7334,7335,7336,7337,12437,12438,7339,7340,17860,17861,12465,12468,12442,12445,17870,12454,12455,17871,14985,14986,17862,17863,16705,16882,17874,17875,17876,17877,7569,7570,7571,17889,7572,7573,7575,12410,7576,7577,7578,17886,17887,12402,12403,12422,12423,12418,12419,12421,17888,7579,7580,7581,12431,12433,12436,12654,14974,14975,14976,17894,17892,17893,18292,18293,18294)
LIMIT 20000000
            """

    # execute the query using spark sql and save as a dataframe
    df_spark = spark.sql(query)
    print('current size: {}'.format(df_spark.count()))
    output_csv = os.path.join("temp_data", "profile" + '_csv')
    df_spark = df_spark.dropna()
    df_spark.repartition(1).write.csv(output_csv, header='true', mode='overwrite')

    # filelist = ['t100_item_4group.csv']
    # for filename in filelist:
    #     csv_file = os.path.join('./', filename)
    #     df_pandas = get_dataframe_from_csv(csv_file)
    #     df = spark.createDataFrame(df_pandas)
    #
    #     outdf = df.join(df_spark, on=['itemid'], how="left")
    #     # sanity check, make sure the size of the df match the expected one
    #     print('{}, current size: {}'.format(filename, outdf.count()))
    #
    #     output_csv = os.path.join("temp_data", os.path.splitext(filename)[0] + '_csv')
    #     outdf.write.csv(output_csv, header='true', mode='overwrite')
