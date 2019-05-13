package org.lfe135.demo8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Hello world!
 *
 */
public class App3 
{
    public static void main( String[] args )
    {
    	try {
    		
    		byte[] type=intToByte(689);
    		String msg = "type@=loginreq/roomid@=288016/";
    		byte[] length=intToByte(4+4+msg.length()+1);
    		
    		ByteArrayOutputStream bos=new ByteArrayOutputStream();
    		bos.write(length);
    		bos.write(length);
    		bos.write(type);
    		bos.write(msg.getBytes());
    		bos.write('\0');
    		bos.flush();
    		
			Socket socket = new Socket("openbarrage.douyutv.com",8601);
			OutputStream outputStream = socket.getOutputStream();
			outputStream.write(bos.toByteArray());
			outputStream.flush();
			InputStream inputStream = socket.getInputStream();
			byte[] input=new byte[1024];
			inputStream.read(input);
			String string = new String(input,12,input.length-12);
			System.out.println(string.substring(0,string.indexOf('\0')));
			
			ByteArrayOutputStream bos2=new ByteArrayOutputStream();
			msg = "type@=joingroup/rid@=288016/gid@=-9999/ ";
			length=intToByte(4+4+msg.length()+1);
			bos2.write(length);
			bos2.write(length);
			bos2.write(type);
			bos2.write(msg.getBytes());
			bos2.write('\0');
    		outputStream.write(bos2.toByteArray());
    		outputStream.flush();
    		inputStream.read(input);
			string = new String(input,12,input.length-12);
			System.out.println(string.substring(0,string.indexOf('\0')));
			new ScheduledThreadPoolExecutor(1).scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					try {
						while(true) {
							Thread.sleep(1000*45);
							ByteArrayOutputStream bos3=new ByteArrayOutputStream();
							byte[] type=intToByte(689);
				    		String msg = "type@=logout/";
				    		byte[] length=intToByte(4+4+msg.length()+1);
				    		bos3.write(length);
				    		bos3.write(length);
				    		bos3.write(type);
				    		bos3.write(msg.getBytes());
				    		bos3.write('\0');
				    		outputStream.write(bos3.toByteArray());
				    		outputStream.flush();
						}
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}}, 0L, 45L, TimeUnit.SECONDS);
			while(true) {
				inputStream.read(input);
				string = new String(input,12,input.length-12);
				String substring = string.substring(0,string.indexOf('\0'));
				System.out.println(substring);
				Matcher matcher = Pattern.compile("\\w+@=([@\\w\\u4e00-\\u9fa5]*/)+").matcher(substring);
				if(matcher.find()) {
					String group = matcher.group();
					if(group.equals("type@=chatmsg/")) {
						do {
							String group1 = matcher.group();
							String[] split = group1.substring(0,group1.length()-1).split("@=");
							String[] values = split[1].split("/");
							for (int i=0;i<values.length;i++) {
								values[i]=values[i].replace("@S", "/").replace("@A", "@");
							}
							split[0]=split[0].replace("@S", "/").replace("@A", "@");
						}while(matcher.find());
					};
					
				}
				while(matcher.find()) {
					System.out.print(matcher.group()+"  ");
				}
				System.out.println();
			}
    	} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
        System.out.println( "Hello World!" );
    }
    public static byte[] intToByte(int val){
	    byte[] b = new byte[4];
	    b[0] = (byte)(val & 0xff);
	    b[1] = (byte)((val >> 8) & 0xff);
	    b[2] = (byte)((val >> 16) & 0xff);
	    b[3] = (byte)((val >> 24) & 0xff);
	    return b;
    }
    public static byte[] intToByte2(int val){
	    byte[] b = new byte[2];
	    b[0] = (byte)(val & 0xff);
	    b[1] = (byte)((val >> 8) & 0xff);
	    return b;
    }
}
