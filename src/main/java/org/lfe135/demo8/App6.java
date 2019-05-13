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

public class App6 {
	public static void main(String[] args) throws InterruptedException {
		DouyuMessageReceiver douyuMessageReceiver = new DouyuMessageReceiver();
		SparkConf conf = new SparkConf().setAppName("douyugift").setMaster("local[2]");
		JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(30));
		context.sparkContext().setLogLevel("ERROR");
		JavaReceiverInputDStream<String> record = context.receiverStream(douyuMessageReceiver);
		JavaDStream<String> giftRecorder=record.filter(rec->rec.contains("type@=dgb/"));
		JavaPairDStream<String,Integer> nameAmount=giftRecorder.mapToPair(gif->{
			String gfid="";
			Integer gfcnt=1;
			Integer hits=1;
			Matcher gfidMatcher=Pattern.compile("gfid@=[\\d]+/").matcher(gif);
			Matcher gfcntMatcher=Pattern.compile("gfcnt@=[\\d]+/").matcher(gif);
			Matcher hitsMatcher=Pattern.compile("hits@=[\\d]+/").matcher(gif);
			if(gfidMatcher.find()) {
				gfid=gfidMatcher.group().replace("gfid@=","").replace("/","");
			}
			if(gfcntMatcher.find()) {
				gfcnt=Integer.parseInt(gfcntMatcher.group().replace("gfcnt@=","").replace("/",""));
			}
			if(hitsMatcher.find()) {
				hits=Integer.parseInt(hitsMatcher.group().replace("hits@=", "").replace("/",""));
			}
			return new Tuple2<String,Integer>(gfid,gfcnt*hits);
		});
		JavaPairDStream<String,Integer> gnc=nameAmount.reduceByKey((gc1,gc2)->gc1+gc2);
		gnc.print(1000);
		context.start();
		context.awaitTermination();
		context.close();
	}
}
