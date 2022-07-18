package org.onedatashare.odsmetadata.config;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class InfluxConfig {

    @Value("${influxdb.token}")
    private String token;

    @Value("${influxdb.url}")
    private String url;

    @Value("${influxdb.org}")
    private String org;

    @Bean
    public InfluxDBClient influxClient() {
        InfluxDBClientOptions influxDBClientOptions = InfluxDBClientOptions.builder()
                .url(this.url)
                .org(this.org)
                .authenticateToken(this.token.toCharArray())
                .build();
        InfluxDBClient client = InfluxDBClientFactory.create(influxDBClientOptions);
//        client.enableGzip();
        return client;
    }
}
