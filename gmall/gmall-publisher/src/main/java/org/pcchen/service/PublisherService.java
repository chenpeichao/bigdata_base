package org.pcchen.service;

import java.util.Map;

/**
 * @author ceek
 * @date 2021/3/6 17:32
 */
public interface PublisherService {
    /**
     * 查询指定时间的日活
     *
     * @param searchDate 查询时间yyyy-MM-dd
     * @param indexName  索引名称
     * @return
     */
    public Integer getDauTotal(String searchDate, String indexName);

    /**
     * 查询指定小时时间的日活
     *
     * @param searchDate 查询时间yyyy-MM-dd
     * @param indexName  索引名称
     * @return
     */
    public Map<String, Object> getDauHour(String searchDate, String indexName);
}