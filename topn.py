from pyspark import SparkConf, SparkContext
import datetime

def myTpoN():
    # 配置SparkContext
    conf = SparkConf().setAppName("topn").setMaster("local[*]")
    sc = SparkContext.getOrCreate(conf)
    #通过路径读取文件夹下符合格式的数据文本
    filepath = 'data/*.txt'

    counts = sc.textFile(filepath) \
        .map(lambda x: x.split(':  '))\
        .filter(lambda x:len(x)>1)\
        .map(lambda x:x[1])\
        .map(lambda x:x.split('-')[1])\
        .map(lambda x: x.rstrip())\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda a,b:a+b)\
        .map(lambda x:(x[1],x[0]))\
        .sortByKey(False)\
        .map(lambda x:(x[1],x[0]))\
        .take(10)

    timec = sc.textFile(filepath)\
        .map(lambda x: x.split(':  '))\
        .filter(lambda x:len(x)>1)\
        .map(lambda x:x[0])\
        .map(lambda x:x.split(' ')[0])\
        .map(lambda x:(x,1))\
        .reduceByKey(lambda a,b:a+b)\
        .map(lambda x:(x[1],x[0]))\
        .sortByKey(False)\
        .map(lambda x:(x[1],x[0]))\
        .take(10)
    """
    for (word, count) in counts:
        print("%s:%i" % (word, count))
    print(counts)

    for (date, count) in timec:
        print("%s:%i" % (date, count))
    print(timec)
    """
    print('被搜索次数最多的关键词是：{}'.format(counts[0][0]))
    print('被访问次数最多的日期是：{}'.format(timec[0][0]))

if __name__ == '__main__':
    t0 = datetime.datetime.now()
    myTpoN()
    t1 = datetime.datetime.now()
    print(t1-t0)