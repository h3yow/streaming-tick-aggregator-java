package com.github.h3yow.tickagg.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(AggregatorProperties.class)
public class AggregatorConfig {
}
