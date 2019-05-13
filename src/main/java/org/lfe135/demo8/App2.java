package org.lfe135.demo8;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/**
 * Hello world!
 *
 */
public class App2 
{
    public static void main( String[] args )
    {
    	SparkConf config=new SparkConf().setMaster("local").setAppName("sparkwordcount1");
    	JavaSparkContext context=new JavaSparkContext(config);
    	JavaRDD<String> textFile = context.textFile("src/main/java/org/lfe135/demo8/App2.java");
    	Pattern compile = Pattern.compile("\\w+");
    	textFile.flatMapToPair(line->{
    		Matcher matcher = compile.matcher(line);
    		List<Tuple2<String,Integer>> list=new ArrayList<>();
    		while(matcher.find()) {
    			list.add(new Tuple2<>(matcher.group(),1));
    		}
    		return list.iterator();
    	})
    		.reduceByKey((a,b)->a+b)
    		.collect()
    		.forEach(pair->System.out.println(pair._1()+":"+pair._2()));
    	context.close();
        System.out.println( "Hello World!" );
    }
}
