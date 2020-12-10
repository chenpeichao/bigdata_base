package org.pcchen.other;

import org.apache.commons.lang.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.Socket;

/**
 * @author ceek
 * @create 2020-12-10 9:48
 **/
public class SocketClient {
    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("localhost", 9999);
        BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        String line = br.readLine();
        while (StringUtils.isNotBlank(line)) {
            System.out.println(line);
            line = br.readLine();
        }
    }
}
