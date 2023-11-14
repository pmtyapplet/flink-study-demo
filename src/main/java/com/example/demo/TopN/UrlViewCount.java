package com.example.demo.TopN;

import lombok.Data;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/8/20 18:26
 */
@Data
public class UrlViewCount {

    public Long start;
    public Long end;
    public Long count;
    public String url;

    public UrlViewCount(Long start, Long end, Long count, String url) {
        this.start = start;
        this.end = end;
        this.count = count;
        this.url = url;
    }
}
