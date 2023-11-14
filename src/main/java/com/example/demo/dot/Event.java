package com.example.demo.dot;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author applet
 * @date 2023/5/21
 */
@Data
@AllArgsConstructor
public class Event {
    public String username;
    public String url;
    public Long timestamp;


    public Event() {
    }
}
