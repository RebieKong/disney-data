## 数据说明
0. 这是什么？
    这是一个在上海迪士尼展示的预计排队时间的数据集，包括了共44个项目每天每5分钟一次的数据。
1. 数据来源
数据来源于从上海迪士尼APP接口获取，详细方法百谷一下。data/origin.zip文件包含了自2017-12-05~2018-12-06共365天的数据。
2. 数据下载
- [原始数据](https://github.com/RebieKong/disney-data/raw/master/data/origin.zip)
- [补全数据](https://github.com/RebieKong/disney-data/raw/master/data/output.zip)

## 我想做什么？

我也不知道能做什么，我希望你能研究点什么，并PR给我

## 清洗流程
1. 将采集实现向下对其到每五分钟一次（采集频率为每5分钟一次，因为一些不可抗力可能会导致数据延迟）
2. 将项目与时间的稀疏矩阵补全，保证每个存在的时间点都包含所有的项目
3. 补全所有未采集的时间点


```java
    SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

	spark.range(1512403200, 1544025600, 300).map((MapFunction<Long, Timestamp>) aLong -> new Timestamp(aLong * 1000), Encoders.TIMESTAMP()).createOrReplaceTempView("lt1");
	spark.read().csv("D:\\disney\\origin.csv")
			.selectExpr(
					"CAST(FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(_c0,'yyyy-MM-dd HH:mm:ss')-1512403200)/300)*300+1512403200) AS timestamp) AS collect_time",
					"CAST(_c1 AS string) AS project",
					"CAST(_c2 AS int) AS queue_length"
			).where("project <> ''").where("project IS NOT NULL")
			.groupBy("collect_time").pivot("project").min("queue_length").createOrReplaceTempView("f1");

	spark.sql("SELECT lt1.value,f1.* FROM lt1 LEFT JOIN f1 ON lt1.value=f1.collect_time")
			.drop("collect_time").withColumnRenamed("value", "collect_time")
			.na().fill(-1)
			.createOrReplaceTempView("f1");

	spark.table("f1").flatMap((FlatMapFunction<Row, RB>) row -> {
		String[] names = row.schema().fieldNames();
		Timestamp time = row.getTimestamp(0);
		return Arrays.stream(names).filter(s -> !s.equals("collect_time")).map(name -> {
			RB r = new RB();
			r.setCollect_time(time);
			r.setProject(name);
			r.setQueue_length(row.getInt(row.schema().fieldIndex(name)));
			return r;
		}).collect(Collectors.toList()).iterator();
	}, Encoders.bean(RB.class));
```
