package org.lfe135.demo8;

import java.sql.Timestamp;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
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
import scala.Tuple3;

public class App10 {
	public static void main(String[] args) throws StreamingQueryException {
		System.setProperty("hadoop.home.dir", "C:\\Users\\DELL\\Downloads\\hadoop-common-2.2.0-bin-master");
		new App8("99999");
		SparkSession session=SparkSession
				.builder()
				.appName("douyudanmu")
				.master("local")
				.getOrCreate();
		session.sparkContext().setLogLevel("ERROR");
		StructType st=new StructType().add("line", "string").add("timestamp","timestamp");
		Dataset<Row> in=session
				.readStream()
				.schema(st)
				.format("text")
				.option("includeTimestamp", true)
				.load("c://testfiles");
		Dataset<Tuple2<String, Timestamp>> tl=in.as(Encoders.tuple(Encoders.STRING(),Encoders.TIMESTAMP()));
		Dataset<Tuple2<String, Timestamp>> timeLineFilted=tl.filter((FilterFunction<Tuple2<String, Timestamp>>)tli->{return tli._1().contains("type@=dbg/");});
		Dataset<Tuple3<Long,Long,Timestamp>> gift=timeLineFilted.map((MapFunction<Tuple2<String, Timestamp>,Tuple3<Long,Long,Timestamp>>)ts->{
			String[] split = ts._1().split("/");
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
			return new Tuple3<Long,Long,Timestamp>(gfid,gfcnt*hits,ts._2());
		}, Encoders.tuple(Encoders.LONG(),Encoders.LONG(),Encoders.TIMESTAMP()));
		Dataset<Row> df = gift.toDF("gfid","gfamo","timestamp");
		Dataset<Row> gfwwm = df.withWatermark("timestamp", "1 minutes");
		RelationalGroupedDataset result= gfwwm.groupBy(functions.window(gfwwm.col("timestamp"),"10 seconds"),gfwwm.col("gfid"));
		Dataset<Row> sum = result.sum("gfamo");
		StreamingQuery start = sum
				.writeStream()
				.outputMode("complete")
				.option("path", "c:\\testfiles")
				.option("checkpointLocation", "c:\\testfiles")
				.format("console")
				.start();
		start.awaitTermination();
	}
}
