package org.lfe135.demo8;

import java.sql.Timestamp;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

public class App11 {
	public static void main(String[] args) throws StreamingQueryException {
		System.setProperty("hadoop.home.dir", "C:\\Users\\DELL\\Downloads\\hadoop-common-2.2.0-bin-master");
		new App8("99999");
		SparkSession session = SparkSession.builder().appName("douyu").master("local").getOrCreate();
		session.sparkContext().setLogLevel("ERROR");
		Dataset<String> line = session.readStream().textFile("C:\\testfiles");
		//StructType schema=new StructType().add("name","string").add("gfid","long").add("pri","double");
		Dataset<Row> json = session.read().json("C:\\testfiles\\test\\part-00000-104a83d4-14ad-47bb-8392-7bae7839d103-c000.json");
		Dataset<String> timeLineFilted=line.filter((FilterFunction<String>)tli->{return tli.contains("type@=dgb/");});
		Dataset<Tuple2<Long,Long>> gift=timeLineFilted.map((MapFunction<String,Tuple2<Long,Long>>)ts->{
			String[] split = ts.split("/");
			Long gfid=0L;
			Long gfcnt=1L;
			Long hits=1L;
			for(String string:split) {
				String[] split2 = string.split("@=");
				switch(split2[0]) {
					case "gfid":
						if(split2.length>1) gfid=Long.parseLong(split2[1]);
						break;
					case "gfcnt":
						if(split2.length>1) gfcnt=Long.parseLong(split2[1]);
						break;
					case "hits":
						if(split2.length>1) hits=Long.parseLong(split2[1]);
						break;
				}
			}
			return new Tuple2<Long,Long>(gfid,gfcnt*hits);
		}, Encoders.tuple(Encoders.LONG(),Encoders.LONG()));
		Dataset<Row> df = gift.toDF("gfid","gfamo");
		RelationalGroupedDataset result= df.groupBy("gfid");
		Dataset<Row> sum = result.sum("gfamo");
		Dataset<Row> join = sum.join(json, sum.col("gfid").equalTo(json.col("gfid")),"left_outer");
		Dataset<Row> withColumn = join.withColumn("sum", join.col("sum(gfamo)").multiply(join.col("pri")));
		withColumn.cache();
		Dataset<Row> select = withColumn.select(functions.sum("sum"));
		StreamingQuery start2 = select
				.writeStream()
				.outputMode("complete")
				//.option("path", "c:\\testfiles")
				//.option("checkpointLocation", "c:\\testfiles")
				.option("numRows",100)
				.format("console")
				.start();
		StreamingQuery start = withColumn
				.writeStream()
				.outputMode("complete")
				//.option("path", "c:\\testfiles")
				//.option("checkpointLocation", "c:\\testfiles")
				.option("numRows",100)
				.format("console")
				.start();
		start2.awaitTermination();
		start.awaitTermination();
		//100鱼丸 20000；超大丸星 20008；小飞蝶 1859 1；幸运钥匙 2096 0.2；弱鸡 20001 0.2；赞 20006 0.1
		//药丸 	20011 0.1；幸运水晶 2095 0.1；幸运戒指 2097 0.5；偏爱 20382 1；
		//办卡 20002 6；飞机 20003 100；20004 火箭 500；超火 20005 2000
	}
}


