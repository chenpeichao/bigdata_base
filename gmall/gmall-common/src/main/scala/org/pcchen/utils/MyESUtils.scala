package org.pcchen.utils

import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory, JestResult}
import io.searchbox.core._

object MyESUtils {
  //  private val ES_HOST = PropertiesUtils.getProperties("es.host")
  //  private val ES_HTTP_PORT = PropertiesUtils.getProperties("es.port")
  //TODO:pcchen 通过动态properties工具类读取
  private val ES_HOST = "http://10.10.32.60"
  private val ES_HTTP_PORT = 9200
  private var factory: JestClientFactory = null

  /**
    * 获取客户端
    *
    * @return jestclient
    */
  def getClient: JestClient = {
    /*synchronized(factory) {
      if (factory == null) build()
      factory.getObject
    }*/
    if (factory == null) build()
    factory.getObject
  }

  /**
    * 关闭客户端
    */
  def close(client: JestClient): Unit = {
    if (!Objects.isNull(client)) try
      client.shutdownClient()
    catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  /**
    * 建立连接
    */
  private def build(): Unit = {
    factory = new JestClientFactory
    factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000).readTimeout(10000).build)
  }


  def main(args: Array[String]): Unit = {
    val jest: JestClient = getClient

    /*//数据查询
    val query = "{\n  \"query\": {\n    \"bool\": {\n      \"must\": [\n        {\"term\": {\n          \"name\": {\n            \"value\": \"li4\"\n          }\n        }}\n      ]\n    }\n  }\n}";
    val search: Search = new Search.Builder(query).build()
    val result: SearchResult = jest.execute(search)
    println(result.getJsonString);*/

    //数据删除
    val query = "{\n  \"query\": {\n    \"bool\": {\n      \"must\": [\n        {\"term\": {\n          \"appid\": {\n            \"value\": \"gmall1205\"\n          }\n        }}\n      ]\n    }\n  }\n}";
    val deleteQuery: DeleteByQuery = new DeleteByQuery.Builder(query).build()
    val deleteQueryResult: JestResult = jest.execute(deleteQuery)
    println(deleteQueryResult.getJsonString);

    /*//数据添加
    val  source="{\n  \"name\":\"li4\",\n  \"age\":456,\n  \"amount\": 250.1,\n  \"phone_num\":\"138***2123\"\n}"
    val index: Index = new Index.Builder(source).index("gmall1205_test").`type`("_doc").build()
    jest.execute(index)*/
    //批量插入---bulk
    close(jest)
  }

  def saveBulkData2ES(dataList: List[Any]) = {
    val jest: JestClient = getClient

    for (item <- dataList) {
      println(item.toString);
    }
    val bulkBuilder = new Bulk.Builder()
    for (doc <- dataList) {
      val index: Index = new Index.Builder(doc).index("gmall_test").`type`("_doc").build()
      bulkBuilder.addAction(index)
    }

    val bulk: Bulk = bulkBuilder.build()
    println("保存了：" + jest.execute(bulk).getItems.size());
    close(jest)
  }
}
