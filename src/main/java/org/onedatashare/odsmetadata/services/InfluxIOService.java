package org.onedatashare.odsmetadata.services;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Service
public class InfluxIOService {

    Logger logger = LoggerFactory.getLogger(InfluxIOService.class);

    @Autowired
    private InfluxDBClient influxDBClient;

    private QueryApi queryApi;

    private final String defaultTime = "-24h";
    private final String rangeStatement = "|> range(start: %s)";
    private final String fromBucket = "from(bucket: \"%s\" ";
    private final String transferNodeBucket = "ODSTransferNodes";
    private final String defaultBucket = String.format(fromBucket, transferNodeBucket);

    @PostConstruct
    public void postConstruct() {
        this.queryApi = influxDBClient.getQueryApi();
    }

}