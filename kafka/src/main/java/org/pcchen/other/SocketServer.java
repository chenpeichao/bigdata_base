package org.pcchen.other;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * socket客户端连接
 *
 * @author ceek
 * @create 2020-12-10 9:16
 **/
public class SocketServer {
    public static void main(String[] args) throws Exception {
        ServerSocket serverSocket = new ServerSocket(9999);

        Socket socket = serverSocket.accept();

        while (true) {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));

            for (int i = 0; i < 20; i++) {
                Thread.currentThread().sleep(200);
                bw.write("hello word" + i);
                System.out.println("hello word" + i);

            }
            bw.write("END");
            //bufferedwrite需要调用newLine方法，
            //flush方法必须添加
            bw.newLine();
            bw.flush();
        }
        /*while(true) {
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(socket.getOutputStream()));

            for(int i = 0; i < 20; i++) {
                Thread.currentThread().sleep(200);
                pw.println("hello word" + i);
                System.out.println("hello word" + i);

            }
            //bufferedwrite需要调用newLine方法，
            //printStream直接调用write
            //flush方法必须添加
            pw.flush();
        }*/

    }
}
