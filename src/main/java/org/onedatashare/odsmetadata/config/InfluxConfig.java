package org.onedatashare.odsmetadata.config;

import com.influxdb.LogLevel;
import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.InfluxDBClientOptions;
import okhttp3.Connection;
import okhttp3.ConnectionPool;
import okhttp3.OkHttpClient;
import okhttp3.Protocol;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

@Configuration
public class InfluxConfig {

    @Value("${influxdb.token}")
    private String token;

    @Value("${influxdb.url}")
    private String url;

    @Value("${influxdb.org}")
    private String org;

    @Value("${spring.application.name}")
    String appName;

    @Bean
    public OkHttpClient.Builder okHttpClient(){
        OkHttpClient.Builder builder = new OkHttpClient.Builder()
                .connectTimeout(600, TimeUnit.SECONDS)
                .readTimeout(600, TimeUnit.SECONDS)
                .callTimeout(600, TimeUnit.SECONDS)
                .protocols(Arrays.asList(Protocol.HTTP_1_1, Protocol.HTTP_2))
                .writeTimeout(600, TimeUnit.SECONDS);
        return builder;
    }

    @Bean
    public InfluxDBClient influxClient(OkHttpClient.Builder okHttpClient) {
        InfluxDBClientOptions influxDBClientOptions = InfluxDBClientOptions.builder()
                .url(this.url)
                .org(this.org)
                .authenticateToken(this.token.toCharArray())
//                .logLevel(LogLevel.BODY)
                .okHttpClient(okHttpClient)
                .build();
        return InfluxDBClientFactory.create(influxDBClientOptions);
    }
}
