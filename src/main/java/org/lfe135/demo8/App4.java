package org.lfe135.demo8;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class App4 {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("douyudanmu").setMaster("local[2]");
		JavaStreamingContext context = new JavaStreamingContext(conf, Durations.minutes(3));
		context.sparkContext().setLogLevel("ERROR");
		JavaReceiverInputDStream<String> ds = context.receiverStream(new DouyuMessageReceiver());
		JavaDStream<String> filter = ds.filter(recorder -> recorder.contains("type@=chatmsg"));
		JavaPairDStream<String, String> mapToPair = filter.mapToPair(line -> {
			Matcher nnMatcher = Pattern.compile("nn@=([？，。\\w\\u4e00-\\u9fa5]*/)+").matcher(line);
			Matcher txtMatcher = Pattern.compile("txt@=([？，。\\w\\u4e00-\\u9fa5]*/)+").matcher(line);
			return new Tuple2<>(nnMatcher.find() ? nnMatcher.group() : "", txtMatcher.find() ? txtMatcher.group() : "");
		});
		JavaPairDStream<String, Iterable<String>> groupByKey = mapToPair.groupByKey();
		groupByKey.cache();
		groupByKey.dstream().print();
		groupByKey.count().dstream().print();
		context.start();
		context.awaitTermination();
		context.close();
		//100鱼丸 20000；超大丸星 20008；小飞蝶 1859 1；幸运钥匙 2096 0.2；弱鸡 20001 0.2；赞 20006 0.1
		//药丸 	20011 0.1；幸运水晶 2095 0.1；幸运戒指 2097 0.5；偏爱 20382 1；
		//办卡 20002 6；飞机 20003 100；20004 火箭 500；超火 20005 2000
	}
}
