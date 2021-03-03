package org.pcchen.constants;

import java.io.IOException;
import java.util.Properties;

/**
 * @author ceek
 * @date 2021/3/3 20:41
 */
public class PropertiesUtils {
    public static String getProperties(String params) {
        Properties properties = new Properties();
        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return properties.getProperty(params);
        }
    }

    public static void main(String[] args) {
        System.out.println(PropertiesUtils.getProperties("es.host"));
    }
}