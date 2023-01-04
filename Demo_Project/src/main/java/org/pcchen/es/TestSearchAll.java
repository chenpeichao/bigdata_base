package org.pcchen.es;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * @author: ceek
 * @create: 2023/1/4 20:24
 */
public class TestSearchAll {
    private static HttpHost[] getHttpHosts(String clientIps, int esHttpPort) {
        String[] clientIpList = clientIps.split(",");
        HttpHost[] httpHosts = new HttpHost[clientIpList.length];
        for (int i = 0; i < clientIpList.length; i++) {
            httpHosts[i] = new HttpHost(clientIpList[i], esHttpPort, "http");
        }
        return httpHosts;
    }
    public static void main(String[] args) throws IOException {
//        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("1", "password"));
//        RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(getHttpHosts("192.168.1.101", 9200))
////                .setHttpClientConfigCallback(
////                        (HttpAsyncClientBuilder httpAsyncClientBuilder) -> httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider))
//        );

        RestHighLevelClient client = new TestSearchAll().getrestHighLevelClient();
        // 搜索请求对象
        SearchRequest searchRequest = new SearchRequest("student");
        // 指定类型
        searchRequest.types("blog");
        // 搜索源构建对象
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        // 搜索方式
        // matchAllQuery搜索全部
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        // 设置源字段过虑,第一个参数结果集包括哪些字段，第二个参数表示结果集不包括哪些字段
//        searchSourceBuilder.fetchSource(new String[]{"title","address","location","timestamp"},new String[]{});
        // 向搜索请求对象中设置搜索源
        searchRequest.source(searchSourceBuilder);
        // 执行搜索,向ES发起http请求
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // 搜索结果
        SearchHits hits = searchResponse.getHits();
        // 匹配到的总记录数
        long totalHits = hits.getTotalHits();
        // 得到匹配度高的文档
        SearchHit[] searchHits = hits.getHits();
        // 日期格式化对象
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        for(SearchHit hit:searchHits){
            // 文档的主键
            String id = hit.getId();
            // 源文档内容
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            String name = (String) sourceAsMap.get("title");
            // 日期
//            Date timestamp = dateFormat.parse((String) sourceAsMap.get("timestamp"));
            System.out.println(name);
        }
        client.close();
    }

    public  RestHighLevelClient getrestHighLevelClient() throws IOException {
        RestHighLevelClient client = null;
        try {
            client = new RestHighLevelClient(
                    RestClient.builder(
//                            new HttpHost(InetAddress.getByName(firstIp),Integer.parseInt(firstPort),"http"),
//                            new HttpHost(InetAddress.getByName(firstIp),Integer.parseInt(firstPort),"http"),
                            new HttpHost(InetAddress.getByName("192.168.1.101"),9200,"http")));

        } catch (UnknownHostException e) {
            e.printStackTrace();
            client.close();
        }
        return client;
    }
}
