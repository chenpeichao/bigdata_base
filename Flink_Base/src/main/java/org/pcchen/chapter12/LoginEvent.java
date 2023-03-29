package org.pcchen.chapter12;

import java.sql.Timestamp;
import java.util.Objects;

/**
 * 登录事件pojo类
 *
 * @author: ceek
 * @create: 2023/3/28 8:16
 */
public class LoginEvent {
    private String userId;
    private String ipAddress;
    private String eventType;
    private Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ipAddress, String eventType, Long timestamp) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getIpAddress() {
        return ipAddress;
    }

    public void setIpAddress(String ipAddress) {
        this.ipAddress = ipAddress;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LoginEvent that = (LoginEvent) o;
        return Objects.equals(userId, that.userId) && Objects.equals(ipAddress, that.ipAddress) && Objects.equals(eventType, that.eventType) && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, ipAddress, eventType, timestamp);
    }
}
