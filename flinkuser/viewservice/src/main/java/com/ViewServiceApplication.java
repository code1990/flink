package com;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 17:49
 * @Author: code1990
 * @Description:
 */
@SpringBootApplication
@EnableEurekaClient
@EnableAutoConfiguration
public class ViewServiceApplication {
    public static void main(String[] args) {

        SpringApplication.run( ViewServiceApplication.class, args );
    }
}
