package org.pcchen.chapter05;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * flink的测试bean
 *
 * @author: ceek
 * @create: 2023/1/2 13:58
 */
public class Event {
    private String name;
    private String urls;
    private Long timestamp;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrls() {
        return urls;
    }

    public void setUrls(String urls) {
        this.urls = urls;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Event() {
    }

    public Event(String name, String urls, Long timestamp) {
        this.name = name;
        this.urls = urls;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", urls='" + urls + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
