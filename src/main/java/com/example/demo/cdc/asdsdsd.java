package com.example.demo.cdc;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2024/2/19 19:22
 */
public class asdsdsd {
    public static void main(String[] args) {


        String s = "12332";
        Matcher matcher = Pattern.compile("[^a-zA-Z0-9]").matcher(s);
        boolean b = matcher.find();

        System.out.println(b);
    }
}
