package org.pcchen.chapter12;

import java.util.Objects;

/**
 * 订单支付相关pojo类
 *
 * @author: ceek
 * @create: 2023/3/28 11:32
 */
public class OrderEvent {
    private String userId;
    private String orderId;
    private String eventType;
    private Long timestamp;

    public OrderEvent() {
    }

    public OrderEvent(String userId, String orderId, String eventType, Long timestamp) {
        this.userId = userId;
        this.orderId = orderId;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
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
        return "OrderEvent{" +
                "userId='" + userId + '\'' +
                ", orderId='" + orderId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderEvent that = (OrderEvent) o;
        return Objects.equals(userId, that.userId) && Objects.equals(orderId, that.orderId) && Objects.equals(eventType, that.eventType) && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, orderId, eventType, timestamp);
    }
}
