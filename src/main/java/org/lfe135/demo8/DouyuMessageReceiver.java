package org.lfe135.demo8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

public class DouyuMessageReceiver extends Receiver<String> {
	private static final long serialVersionUID = 1L;

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

	@Override
	public void onStart() {
		new Thread(this::receive).start();
	}

	@Override
	public void onStop() {
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

	public DouyuMessageReceiver() {
		super(StorageLevel.MEMORY_AND_DISK_2());
	}

	private void receive() {
		Socket socket = null;
		String userInput = null;
		try {
			socket = new Socket("openbarrage.douyutv.com", 8601);
			OutputStream outputStream = socket.getOutputStream();
			InputStream inputStream = socket.getInputStream();
			outputStream.write(getSend("type@=loginreq/roomid@=99999/"));
			outputStream.write(getSend("type@=joingroup/rid@=99999/gid@=-9999/"));
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
			while (!isStopped()) {
				byte[] len = new byte[4];
				for (int i = 0; i < 4; i++) {
					len[i] = (byte) inputStream.read();
				}
				int length = byte2Int(len);
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
