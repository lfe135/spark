package org.lfe135.demo8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import scala.Tuple2;

public class App4 {
	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setAppName("douyu").setMaster("local[2]");
		JavaStreamingContext context = new JavaStreamingContext(conf, Durations.minutes(3));
		context.sparkContext().setLogLevel("ERROR");
		JavaReceiverInputDStream<String> ds = context.receiverStream(new JavaCustomReceiver());
		JavaDStream<String> filter = ds.filter(recorder -> recorder.contains("type@=chatmsg"));
		JavaPairDStream<String, String> mapToPair = filter.mapToPair(line -> {
			Matcher matcher1 = Pattern.compile("nn@=([？，。\\w\\u4e00-\\u9fa5]*/)+").matcher(line);
			Matcher matcher2 = Pattern.compile("txt@=([？，。\\w\\u4e00-\\u9fa5]*/)+").matcher(line);
			return new Tuple2<>(matcher1.find() ? matcher1.group() : "", matcher2.find() ? matcher2.group() : "");
		});
		JavaPairDStream<String, Iterable<String>> groupByKey = mapToPair.groupByKey();
		groupByKey.persist();
		groupByKey.dstream().print(1000);
		groupByKey.count().dstream().print();
		context.start();
		context.awaitTermination();
		context.close();
		System.out.println("Hello World!");
	}

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
}

class JavaCustomReceiver extends Receiver<String> {

	private static final long serialVersionUID = 1L;
	String host = null;
	int port = -1;

	public JavaCustomReceiver() {
		super(StorageLevel.MEMORY_AND_DISK_2());
	}

	@Override
	public void onStart() {
		new Thread(this::receive).start();
	}

	@Override
	public void onStop() {
	}

	private void receive() {
		Socket socket = null;
		String userInput = null;

		try {
			socket = new Socket("121.9.245.181", 8601);

			OutputStream outputStream = socket.getOutputStream();
			InputStream inputStream = socket.getInputStream();
			outputStream.write(App4.getSend("type@=loginreq/roomid@=99999/"));
			outputStream.write(App4.getSend("type@=joingroup/rid@=99999/gid@=-9999/"));

			new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					try {
						outputStream.write(App4.getSend("type@=mrkl/"));
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}, 0L, 45L, TimeUnit.SECONDS);

			while (!isStopped()) {
				byte[] len = new byte[4];
				for (int i = 0; i < 4; i++) {
					len[i] = (byte) inputStream.read();
				}
				int length = App4.byte2Int(len);
				byte[] input = new byte[length];
				int i = 0;
				do {
					int available = inputStream.available();
					if (available < length) {
						inputStream.read(input, i, available);
						i = available;
						length -= available;
					} else {
						inputStream.read(input, i, length);
						break;
					}
				} while (true);
				userInput = new String(input);
				store(userInput);
			}
			socket.close();
			restart("Trying to connect again");
		} catch (ConnectException ce) {
			restart("Could not connect", ce);
		} catch (Throwable t) {
			restart("Error receiving data", t);
		}
	}
}
