package com.github.h3yow.streamingtickaggregatorjava;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.github.h3yow.streamingtickaggregatorjava.config.AggregatorProperties;

@SpringBootApplication
@EnableConfigurationProperties(AggregatorProperties.class)
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }
}