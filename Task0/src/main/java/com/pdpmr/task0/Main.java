package com.pdpmr.task0;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Main {



    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        prop.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));

        // get the property value and print it out
        System.out.println(prop.getProperty("k-neighborhoods"));

    }
}
