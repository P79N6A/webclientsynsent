package com.example;

import io.vertx.core.buffer.Buffer;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.InetAddress;
import java.net.URL;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit test for simple App.
 */
@ExtendWith(VertxExtension.class)
public class AppTest {
    /**
     * test vertx.
     */
    @Test
    public void vertx1() {
        try {
            String ip = "127.0.0.1";
            String[] ipArray = ip.split("\\.");
            Buffer bf = Buffer.buffer();
            for (int i = 0; i < ipArray.length; i++) {
                bf.appendByte((byte) Integer.parseInt(ipArray[i]));
                System.out.println(ipArray[i] + ":" + (byte) Integer.parseInt(ipArray[i]));
            }
            for (byte b : bf.getBytes()) {
                System.out.println(b);
            }
            System.out.println(InetAddress.getByName("127.0.0.1").getHostName() + ":" + InetAddress.getByAddress(bf.getBytes()).getHostName());
            String errMsg = "/fi/";
            assertEquals(new URL(new URL("http://www.baidu.com"), "fi/"), new URL("http://www.baidu.com/fi/"));
        } catch (Exception e) {
        }
    }
}
