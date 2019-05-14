package org.lfe135.demo8;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

public class App7 {
	public static void main(String[] args) throws StreamingQueryException {
		System.setProperty("hadoop.home.dir", "C:\\Users\\DELL\\Downloads\\hadoop-common-2.2.0-bin-master");
		SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();
		session.sparkContext().setLogLevel("ERROR");
		Dataset<Row> textFile = session.readStream().text("C:\\testfiles");
		Dataset<Row> count = textFile.groupBy("value").count();
		StreamingQuery start = count.writeStream().outputMode("complete").format("console").start();
		start.awaitTermination();
	}
}

class LineAndLength {
	private String line;
	private Long length;

	public String getLine() {
		return line;
	}

	public void setLine(String line) {
		this.line = line;
	}

	public Long getLength() {
		return length;
	}

	public void setLength(Long length) {
		this.length = length;
	}

}
