package org.pcchen.service.impl;

import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.pcchen.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author ceek
 * @date 2021/3/6 17:32
 */
@Service
public class PublisherServiceImpl implements PublisherService {
    @Autowired
    private JestClient jestClient;

    /**
     * 查询指定时间的日活
     *
     * @param searchDate 查询时间yyyy-MM-dd
     * @param indexName  索引名称
     * @return
     */
    public Integer getDauTotal(String searchDate, String indexName) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", searchDate));

        SearchSourceBuilder query = searchSourceBuilder.query(boolQueryBuilder);
        System.out.println("查询语句为" + query.toString());
        Search search = new Search.Builder(query.toString()).addIndex(indexName).addType("_doc").build();
        int total = 0;
        try {
            SearchResult searchResult = jestClient.execute(search);
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }

    /**
     * 查询指定小时时间的日活
     *
     * @param searchDate 查询时间yyyy-MM-dd
     * @param indexName  索引名称
     * @return
     */
    public Map<String, Object> getDauHour(String searchDate, String indexName) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        boolQueryBuilder.filter(new TermQueryBuilder("logDate", searchDate));
        SearchSourceBuilder query = searchSourceBuilder.query(boolQueryBuilder);

        TermsBuilder groupByHourAgg = AggregationBuilders.terms("groupByHour").field("logHour.keyword").size(24);
        query.aggregation(groupByHourAgg);
        System.out.println("查询语句为" + query.toString());
        Search search = new Search.Builder(query.toString()).addIndex(indexName).addType("_doc").build();
        Map<String, Object> resultMap = new HashMap<String, Object>();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> groupByHourList = searchResult.getAggregations().getTermsAggregation("groupByHour").getBuckets();
            Iterator<TermsAggregation.Entry> iterator = groupByHourList.iterator();
            while (iterator.hasNext()) {
                TermsAggregation.Entry next = iterator.next();
                resultMap.put(next.getKey(), next.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resultMap;
    }

    /**
     * 查询指定时间的日活
     * @param searchDate    查询时间yyyy-MM-dd
     * @param indexName     索引名称
     * @return
     *//*
    public Integer getDauTotal(String searchDate, String indexName) {
    //也可通过SearchSourceBuilder构建查询
        String query = "{\n" +
                "  \"query\": {\n" +
                "    \"bool\": {\n" +
                "      \"filter\": {\n" +
                "        \"term\": {\n" +
                "          \"logDate\": \""+searchDate+"\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        Search searchBuiler = new Search.Builder(query).addIndex(indexName).addType("_doc").build();

        Integer total = 0;
        try {
            SearchResult searchResult = jestClient.execute(searchBuiler);
            total = searchResult.getTotal();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return total;
    }*/
}