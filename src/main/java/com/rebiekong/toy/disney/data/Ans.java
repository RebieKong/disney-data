package com.rebiekong.toy.disney.data;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class Ans {
    public static void main(String... args) {
        Properties userProperties = new Properties();
        userProperties.setProperty("user", "root");
        userProperties.setProperty("password", "root");
        SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

        spark.range(1512403200, 1544025600, 300).map((MapFunction<Long, Timestamp>) aLong -> new Timestamp(aLong * 1000), Encoders.TIMESTAMP()).createOrReplaceTempView("lt1");

        
        spark.read().csv("C:\\Users\\Dell\\Desktop\\log.csv")
                .selectExpr(
                        "CAST(FROM_UNIXTIME(FLOOR((UNIX_TIMESTAMP(_c0,'yyyy-MM-dd HH:mm:ss')-1512403200)/300)*300+1512403200) AS timestamp) AS collect_time",
                        "CAST(_c1 AS string) AS project",
                        "CAST(_c2 AS int) AS queue_length"
                ).where("project <> ''").where("project IS NOT NULL")
                .groupBy("collect_time").pivot("project").min("queue_length").createOrReplaceTempView("f1");
        spark.sql("SELECT lt1.value,f1.* FROM lt1 LEFT JOIN f1 ON lt1.value=f1.collect_time")
                .drop("collect_time").withColumnRenamed("value", "collect_time")
                .na().fill(-1L)
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
        }, Encoders.bean(RB.class))
                .write()
                .mode(SaveMode.Overwrite)
                .option("truncate", "true")
                .jdbc("jdbc:mysql://192.168.30.172:3306/my_datas", "disney2", userProperties);
    }

    public static class RB {
        public Timestamp collect_time;
        public String project;
        public int queue_length;

        public Timestamp getCollect_time() {
            return collect_time;
        }

        public void setCollect_time(Timestamp collect_time) {
            this.collect_time = collect_time;
        }

        public String getProject() {
            return project;
        }

        public void setProject(String project) {
            this.project = project;
        }

        public int getQueue_length() {
            return queue_length;
        }

        public void setQueue_length(int queue_length) {
            this.queue_length = queue_length;
        }
    }
}
