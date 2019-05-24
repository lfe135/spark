package org.lfe135.demo8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;

import scala.Tuple2;

public class App13 {
	public static void main(String[] args) throws InterruptedException {
		System.setProperty("hadoop.home.dir","C:\\Users\\DELL\\Downloads\\hadoop-common-2.2.0-bin-master");
		SparkConf conf=new SparkConf()
				.setAppName("douyudanmu13")
				.setMaster("local[2]");
		JavaStreamingContext context=new JavaStreamingContext(conf,new Duration(5*1000));
		context.checkpoint("C:\\testfiles");
		context.sparkContext().setLogLevel("ERROR");
		JavaReceiverInputDStream<String> in = context.receiverStream(new MyReceiver());
		JavaDStream<String> gi = in.filter(lin->lin.contains("type@=dgb/"));
		JavaPairDStream<Long, Long> ga = gi.mapToPair(gl->{
			String[] split=gl.split("/");
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
			return new Tuple2<Long,Long>(gfid,gfcnt*hits);
		});
		JavaPairDStream<Long, Long> result = ga.reduceByKey((a1,a2)->a1+a2);
		result.updateStateByKey((values,state)->{
			Long v=(Long) state.or(0L);
			for(Long vl:values) {
				v+=vl;
			}
			return Optional.of(v);	
		}).print(100);
		result.print(100);
		context.start();
		context.awaitTermination();
		context.close();
	}
}

class MyReceiver extends Receiver<String> implements Runnable {

	private static final long serialVersionUID = 1L;

	public MyReceiver() {
		super(StorageLevels.MEMORY_ONLY);
	}

	@Override
	public void onStart() {
		//tp.execute(this);
		new Thread(this).start();
	}

	@Override
	public void onStop() {
	}

	@Override
	public void run() {
		try(Socket socket = new Socket("openbarrage.douyutv.com", 8601)){
			OutputStream outputStream = socket.getOutputStream();
			InputStream inputStream = socket.getInputStream();
			String head1="type@=loginreq/roomid@=99999/";
			String head2="type@=joingroup/rid@=99999/gid@=-9999/";
			outputStream.write(getSend(head1));
			outputStream.write(getSend(head2));
			new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					try {
						outputStream.write(getSend("type@=mrkl/"));
					}catch(Exception e) {
						
					}
				}
			},0L, 45L, TimeUnit.SECONDS);
			while (true) {
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
				store(new String(input));
			}
		} catch (Exception e) {
			 restart("Error receiving data", e);
		}
	}

	public byte[] getSend(String msg) {
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

	public byte[] int2Byte(int val) {
		byte[] b = new byte[4];
		b[0] = (byte) (val & 0xff);
		b[1] = (byte) ((val >> 8) & 0xff);
		b[2] = (byte) ((val >> 16) & 0xff);
		b[3] = (byte) ((val >> 24) & 0xff);
		return b;
	}

	public int byte2Int(byte[] bytes) {
		int value = 0;
		value = ((bytes[3] & 0xff) << 24) | ((bytes[2] & 0xff) << 16) | ((bytes[1] & 0xff) << 8) | (bytes[0] & 0xff);
		return value;
	}

}