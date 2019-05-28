package org.lfe135.demo8;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Date;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.expressions.javalang.typed;

import scala.Tuple2;
import scala.Tuple3;

public class App14 {
	public static byte[] getSend(String msg) {
		byte[] type = int2Byte(689);
		byte[] length = int2Byte(4 + 4 + msg.length() + 1);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try {
			bos.write(length);
			bos.write(length);
			bos.write(type);
			bos.write(msg.getBytes());
			bos.write('\0');
			bos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bos.toByteArray();
	}

	public static byte[] int2Byte(int val) {
		byte[] b = new byte[4];
		b[0] = (byte) (val & 0xff);
		b[1] = (byte) ((val >> 8) & 0xff);
		b[2] = (byte) ((val >> 16) & 0xff);
		b[3] = (byte) ((val >> 24) & 0xff);
		return b;
	}

	public static int byte2Int(byte[] bytes) {
		int value = 0;
		value = ((bytes[3] & 0xff) << 24) | ((bytes[2] & 0xff) << 16) | ((bytes[1] & 0xff) << 8) | (bytes[0] & 0xff);
		return value;
	}

	public static void getMessage() {
		try (Socket socket = new Socket("openbarrage.douyutv.com", 8601)) {
			OutputStream outputStream = socket.getOutputStream();
			InputStream inputStream = socket.getInputStream();
			String head1 = "type@=loginreq/roomid@=99999/";
			String head2 = "type@=joingroup/rid@=99999/gid@=-9999/";
			outputStream.write(getSend(head1));
			outputStream.write(getSend(head2));
			new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					try {
						outputStream.write(getSend("type@=mrkl/"));
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}, 0L, 45L, TimeUnit.SECONDS);
			FileWriter fileWriter = new FileWriter(
					"c:\\testfiles\\" + new Date().toString().replace(" ", "").replace(":", ""));
			int a = 0;
			while (true) {
				a++;
				byte[] len = new byte[4];
				for (int i1 = 0; i1 < 4; i1++) {
					len[i1] = (byte) inputStream.read();
				}
				int length = byte2Int(len);
				byte[] input = new byte[length];
				int i = 0;
				while (true) {
					int available = inputStream.available();
					if (available < length) {
						inputStream.read(input, i, available);
						i = available;
						length -= available;
					} else {
						inputStream.read(input, i, length);
						break;
					}
				}
				if (a > 1000) {
					fileWriter.close();
					fileWriter = new FileWriter(
							"c:\\testfiles\\" + new Date().toString().replace(" ", "").replace(":", ""));
					a = 0;
				}
				fileWriter.write(new String(input));
				fileWriter.write('\n');
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Dataset<String> inputDataset() {
		System.setProperty("hadoop.home.dir", "c:\\Users\\DELL\\Downloads\\hadoop-common-2.2.0-bin-master");
		SparkSession session = SparkSession.builder().master("local[*]").getOrCreate();
		session.sparkContext().setLogLevel("ERROR");
		return session.read().format("text").schema("value STRING").load("c:\\testfiles").as(Encoders.STRING());
	}

	public static void userCount() {
		Pattern p = Pattern.compile("nn@=[^/(@=)]*/");
		Dataset<String> line = inputDataset();
		System.out.println(
				line.filter((FilterFunction<String>) l -> l.contains("nn@=")).map((MapFunction<String, String>) l -> {
					String s = null;
					Matcher m = p.matcher(l);
					if (m.find())
						s = m.group();
					return s;
				}, Encoders.STRING()).groupByKey((MapFunction<String, String>) l -> l, Encoders.STRING()).count().count());
	}

	public static void chatmsg() {
		Dataset<String> line = inputDataset();
		Dataset<String> chatmsg = line.filter((FilterFunction<String>) l -> l.contains("type@=chatmsg"));
		chatmsg.cache();
		System.out.println(chatmsg.count());
		Dataset<Tuple2<String, String>> chatmsgUsers = chatmsg.map((MapFunction<String, Tuple2<String, String>>) l -> {
			String[] split = l.split("/");
			String nn = "";
			String txt = "";
			for (String s : split) {
				String[] split2 = s.split("@=");
				if (split2.length > 0) {

					switch (split2[0]) {
					case "nn":
						if (split2.length > 1) {
							nn = split2[1];
						}
						break;
					case "txt":
						if (split2.length > 1) {
							txt = split2[1];
						}
						break;
					}
				}
			}
			return new Tuple2<String, String>(nn, txt);
		}, Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
				.groupByKey((MapFunction<Tuple2<String, String>, String>) l -> l._1(), Encoders.STRING())
				.agg(new StringAggregator().toColumn());
		chatmsgUsers.cache();
		chatmsgUsers.write().json("c:\\testfiles\\test");
		System.out.println(chatmsgUsers.count());
	}

	public static void gift() {
		Dataset<String> line = inputDataset();
		Dataset<Tuple3<String, Long, Long>> gift = line.filter((FilterFunction<String>) l -> l.contains("type@=dgb"))
				.map((MapFunction<String, Tuple3<String, Long, Long>>) l -> {
					String[] split = l.split("/");
					Long gfid = 0L;
					Long gfcnt = 0L;
					Long hits = 0L;
					String nn = "";
					for (String s : split) {
						String[] split2 = s.split("@=");
						switch (split2[0]) {
						case "gfid":
							if (split2.length > 1)
								gfid = Long.parseLong(split2[1]);
							break;
						case "gfcnt":
							if (split2.length > 1)
								gfcnt = Long.parseLong(split2[1]);
							break;
						case "hits":
							if (split2.length > 1)
								hits = Long.parseLong(split2[1]);
							break;
						case "nn":
							if (split2.length > 1)
								nn = split2[1];
							break;
						}
					}
					return new Tuple3<String, Long, Long>(nn, gfid, gfcnt * hits);
				}, Encoders.tuple(Encoders.STRING(), Encoders.LONG(), Encoders.LONG()));
		gift.cache();
		System.out.println(
				gift.groupByKey((MapFunction<Tuple3<String, Long, Long>, String>) l -> l._1(), Encoders.STRING())
						.agg(typed.sumLong((MapFunction<Tuple3<String, Long, Long>, Long>) l -> l._3())).count());
		gift.groupByKey((MapFunction<Tuple3<String, Long, Long>, Long>) l -> l._2(), Encoders.LONG())
				.agg(typed.sumLong((MapFunction<Tuple3<String, Long, Long>, Long>) l -> l._3())).show(100);
	}
}

class StringAggregator extends Aggregator<Tuple2<String, String>, String, String> {

	private static final long serialVersionUID = 1L;

	@Override
	public Encoder<String> bufferEncoder() {
		return Encoders.STRING();
	}

	@Override
	public String finish(String reduction) {
		return reduction;
	}

	@Override
	public String merge(String b1, String b2) {
		return b1 + b2;
	}

	@Override
	public Encoder<String> outputEncoder() {
		return Encoders.STRING();
	}

	@Override
	public String reduce(String b, Tuple2<String, String> a) {
		return b + "," + a._2();
	}

	@Override
	public String zero() {
		return "";
	}

}
