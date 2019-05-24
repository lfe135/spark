package org.lfe135.demo8;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Date;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class App8 implements Runnable {
	private String rid = "99999";

	public App8(String rid) {
		this.rid=rid;
		new Thread(this).start();
	}
	

	public App8() {
		new Thread(this).start();
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

	@Override
	public void run() {
		Socket socket;
		try {
			socket = new Socket("openbarrage.douyutv.com", 8601);
			OutputStream outputStream = socket.getOutputStream();
			InputStream inputStream = socket.getInputStream();
			String head1="type@=loginreq/roomid@="+rid+"/";
			String head2="type@=joingroup/rid@="+rid+"/gid@=-9999/";
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
			String fileName = "c:\\testfiles\\" + new Date().toString().replace(" ", "").replace(":", "");
			File file = new File(fileName);
			file.createNewFile();
			FileWriter fileWriter = new FileWriter(file);
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
					fileName = "c:\\testfiles\\" + new Date().toString().replace(" ", "").replace(":", "");
					file = new File(fileName);
					file.createNewFile();
					fileWriter = new FileWriter(file);
					a = 0;
				}
				fileWriter.write(new String(input));
				fileWriter.write('\n');
			}
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
}
