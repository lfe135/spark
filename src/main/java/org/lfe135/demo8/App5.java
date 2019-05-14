package org.lfe135.demo8;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
/**
 * Hello world!
 *
 */
public class App5 
{
    public static void main( String[] args ) throws UnknownHostException, IOException
    {
    	  Socket socket = new Socket("openbarrage.douyutv.com",8601);
	      OutputStream outputStream = socket.getOutputStream();
	    //  outputStream.write(App4.getSend("type@=loginreq/roomid@=99999/"));
	      InputStream inputStream = socket.getInputStream();
	     // outputStream.write(App4.getSend("type@=joingroup/rid@=99999/gid@=-9999/"));
	     // byte[] byte2=new byte[50];
	      FileWriter fileWriter = new FileWriter("C:\\Users\\DELL\\1.txt");
	      byte[] byte1=new byte[0];
	      do {
	    	  //System.out.println(new String(byte1));
	    	  fileWriter.write(new String(byte1)+'\n');
	    	  byte1=new byte[inputStream.available()];
	      }while(inputStream.read(byte1)!=-1);
	     // while(inputStream.read(byte2)!=-1) {
	      fileWriter.close();
	    	  //fileWriter.write(new String(byte2));
	      //}
	      //fileWriter.close();
	      socket.close();
        System.out.println( "Hello World!" );
    }
}
