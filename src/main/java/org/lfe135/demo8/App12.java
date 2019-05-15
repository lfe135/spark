package org.lfe135.demo8;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class App12 {
	public static void main(String[] args) throws StreamingQueryException {
		System.setProperty("hadoop.home.dir", "C:\\Users\\DELL\\Downloads\\hadoop-common-2.2.0-bin-master");
		SparkSession session = SparkSession.builder().appName("douyu").master("local").getOrCreate();
		session.sparkContext().setLogLevel("ERROR");
		List<String> jsonData = Arrays.asList(
		        "[\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"100鱼丸\",\r\n" + 
		        "        \"gfid\": \"20000\",\r\n" + 
		        "        \"pri\": \"0\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"超大丸星\",\r\n" + 
		        "        \"gfid\": \"20008\",\r\n" + 
		        "        \"pri\": \"0\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"小飞蝶\",\r\n" + 
		        "        \"gfid\": \"1859\",\r\n" + 
		        "        \"pri\": \"1\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"幸运钥匙\",\r\n" + 
		        "        \"gfid\": \"2096\",\r\n" + 
		        "        \"pri\": \"0.2\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"弱鸡\",\r\n" + 
		        "        \"gfid\": \"20001\",\r\n" + 
		        "        \"pri\": \"0.2\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"赞\",\r\n" + 
		        "        \"gfid\": \"20006\",\r\n" + 
		        "        \"pri\": \"0.1\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"药丸\",\r\n" + 
		        "        \"gfid\": \"20011\",\r\n" + 
		        "        \"pri\": \"0.1\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"幸运水晶\",\r\n" + 
		        "        \"gfid\": \"2095\",\r\n" + 
		        "        \"pri\": \"0.1\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"幸运戒指\",\r\n" + 
		        "        \"gfid\": \"2097\",\r\n" + 
		        "        \"pri\": \"0.5\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"偏爱\",\r\n" + 
		        "        \"gfid\": \"20382\",\r\n" + 
		        "        \"pri\": \"1\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"办卡\",\r\n" + 
		        "        \"gfid\": \"20002\",\r\n" + 
		        "        \"pri\": \"6\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"飞机\",\r\n" + 
		        "        \"gfid\": \"20003\",\r\n" + 
		        "        \"pri\": \"100\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"火箭\",\r\n" + 
		        "        \"gfid\": \"20004\",\r\n" + 
		        "        \"pri\": \"500\"\r\n" + 
		        "    },\r\n" + 
		        "    {\r\n" + 
		        "        \"name\": \"超火\",\r\n" + 
		        "        \"gfid\": \"20005\",\r\n" + 
		        "        \"pri\": \"2000\"\r\n" + 
		        "    }\r\n" + 
		        "]");
		Dataset<String> anotherPeopleDataset = session.createDataset(jsonData, Encoders.STRING());
		Dataset<Row> anotherPeople = session.read().json(anotherPeopleDataset);
		anotherPeople.printSchema();
		anotherPeople.show();
		anotherPeople.write().json("c:\\testfiles\\test");
		//100鱼丸 20000；超大丸星 20008；小飞蝶 1859 1；幸运钥匙 2096 0.2；弱鸡 20001 0.2；赞 20006 0.1
		//药丸 	20011 0.1；幸运水晶 2095 0.1；幸运戒指 2097 0.5；偏爱 20382 1；
		//办卡 20002 6；飞机 20003 100；20004 火箭 500；超火 20005 2000
	}
}


