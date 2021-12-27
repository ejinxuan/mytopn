# mytopn
2021大数据工程作业

使用fake.py生成数据

使用topn.py进行数据处理，需在集群上提交

topn.py文件提交说明：
在hdfs上提交文件后需将代码第9行改为对应地址。
1.单线程模式
	将第6行setMaster参数改为“local”
2.多线程模式
	将第6行setMaster参数改为“local[*]”
3.分布式模式
	将第6行setMaster函数注释掉
	
提交时，若为分布式模式，使用命令
	spark-submit --master yarn executor-cores 4 /.../topn.py
若为单线程或多线程模式，使用命令
	spark-submit /.../topn.py