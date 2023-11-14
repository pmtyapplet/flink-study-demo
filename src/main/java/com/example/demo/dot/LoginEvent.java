package com.example.demo.dot;

import lombok.Data;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/11/8 19:03
 */
@Data
public class LoginEvent {

    public String userId;

    public String ipAddress;

    public String eventType;

    public Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(String userId, String ipAddress, String eventType, Long timestamp) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.eventType = eventType;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId='" + userId + '\'' +
                ", ipAddress='" + ipAddress + '\'' +
                ", eventType='" + eventType + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
