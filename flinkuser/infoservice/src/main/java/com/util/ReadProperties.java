package com.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

/**
 * @program: flinkuser
 * @Date: 2019-10-28 22:43
 * @Author: code1990
 * @Description:
 */
public class ReadProperties {
    public final static Config config = ConfigFactory.load("test.properties");
    public static String getKey(String key){
        return config.getString(key).trim();
    }
    public static String getKey(String key,String filename){
        Config config =  ConfigFactory.load(filename);
        return config.getString(key).trim();
    }
}
