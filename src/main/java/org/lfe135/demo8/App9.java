package org.lfe135.demo8;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.KeyValueGroupedDataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import scala.Tuple2;

public class App9 {
	public static void main(String[] args) throws StreamingQueryException {
		System.setProperty("hadoop.home.dir", "C:\\Users\\DELL\\Downloads\\hadoop-common-2.2.0-bin-master");
		SparkSession session = SparkSession.builder().appName("douyu").master("local").getOrCreate();
		session.sparkContext().setLogLevel("ERROR");
		Dataset<String> line = session.readStream().textFile("C:\\testfiles");
		Dataset<String> lfds = line.filter((FilterFunction<String>)lin->lin.contains("type@=dgb/"));
		Dataset<Gift> giftds = lfds.map((MapFunction<String,Gift>)lfd->{
			String[] split = lfd.split("/");
			Gift gift = new Gift();
			for (String string : split) {
				String[] split2 = string.split("@=");
				switch (split2[0]) {
				case "gfid":
					if(split2.length>1) gift.setGfid(Long.parseLong(split2[1]));
					break;
				case "gfcnt":
					if(split2.length>1) gift.setGfcnt(Long.parseLong(split2[1]));
					break;
				case "hits":
					if(split2.length>1) gift.setHits(Long.parseLong(split2[1]));
					break;
				default:
					break;
				}
			}
			return gift;
		}, Encoders.bean(Gift.class));
		KeyValueGroupedDataset<Long, Gift> giftidds = giftds.groupByKey((MapFunction<Gift,Long>)gif->gif.getGfid(), Encoders.LONG());
		Dataset<Tuple2<Long, Long>> giftagg = giftidds.agg(typed.sumLong((MapFunction<Gift, Long>)gif->gif.getGfcnt()*gif.getHits()));
		StreamingQuery start = giftagg.writeStream().outputMode("append").option("checkpointLocation", "c:\\testfiles") .option("path", "c:\\testfiles").format("json").start();
		start.awaitTermination();
	}
}


