package org.pcchen.utils

import java.io.InputStreamReader
import java.util.Properties

/**
  *
  * @author ceek
  * @create 2021-03-02 14:39
  **/
object PropertiesUtils {
  def main(args: Array[String]): Unit = {
    val properties: Properties = PropertiesUtils.load("config.properties")

    println(properties.getProperty("kafka.broker.list"))
  }

  def load(propertieName: String): Properties = {
    val prop = new Properties();
    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))
    prop
  }
}
