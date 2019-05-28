package org.lfe135.demo8;

import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	SparkConf config=new SparkConf().setMaster("local").setAppName("sparkwordcount1");
    	JavaSparkContext context=new JavaSparkContext(config);
    	JavaRDD<String> textFile = context.textFile("src/main/java/org/lfe135/demo8/App.java");
    	textFile.flatMapToPair(line->{
    		StringTokenizer stringTokenizer = new StringTokenizer(line);
    		List<Tuple2<String,Integer>> list=new ArrayList<>();
    		while(stringTokenizer.hasMoreTokens()) {
    			Tuple2<String,Integer> tuple2 = new Tuple2<>(stringTokenizer.nextToken(),1);
    			list.add(tuple2);
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
class Solution {
    public int lastStoneWeight(int[] stones) {
        for(int i=0;i<stones.length-1;i++) {
            for(int j=0;j<stones.length-1-i;j++){
                if(stones[j]>stones[j+1]){
                    int t=stones[j];
                    stones[j]=stones[j+1];
                    stones[j+1]=t;
                }
            }
        }
        for(int i=0;i<stones.length-1;i++){
            if(stones[i]==stones[i+1]){
                stones[i]=0;
                stones[i+1]=0;
                i++;
            }else{
                int t=stones[i]-stones[i+1];
                if(t>0){
                    stones[i+1]=t;
                }else{
                    stones[i+1]=t*(-1);
                }
            }
        }
        return stones[stones.length-1];
    }
}
