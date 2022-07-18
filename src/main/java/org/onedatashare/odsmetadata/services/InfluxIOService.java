package org.onedatashare.odsmetadata.services;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.OnboardingRequest;
import com.influxdb.client.domain.OnboardingResponse;
import com.influxdb.exceptions.UnprocessableEntityException;
import com.influxdb.query.FluxTable;
import com.influxdb.query.dsl.Flux;
import com.influxdb.query.dsl.functions.restriction.Restrictions;
import org.onedatashare.odsmetadata.model.InfluxData;
import org.onedatashare.odsmetadata.model.JobStatisticDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

@Service
public class InfluxIOService {

    Logger logger = LoggerFactory.getLogger(InfluxIOService.class);

    @Autowired
    private InfluxDBClient influxDBClient;

    private final String ODS_USER="ods_user";
    private final String ODS_JOB_ID="jobId";

    private final String fromBucket = "from(bucket: \"%s\") ";
    private final String rangeStatement = "|> range(start: %s) ";
    private final String defaultBucket = "OdsTransferNodes";

    private final String filterByMeasurement = "|> filter(fn: (r) => r[\"_measurement\"] == \"transfer_data\") ";
    private final String filterByNode = "|> filter(fn: (r) => r[\"APP_NAME\"] == \"%s\") ";
    private final String filterByUserName = "|> filter(fn: (r) => r[\"ods_user\"] == \"%s\") ";

    @Value("influxdb.org")
    private String influxOrg;

    @Value("influxdb.token")
    private String token;

    @Value("influxdb.measurement")
    private String measurement;

    /**
     * Load all of a users data that ran through AWS
     *
     * @param userName
     * @return
     */
    public List<InfluxData> getAllUserInfluxData(String userName) {
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        List<InfluxData> retList = new ArrayList<>();
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(-1L, ChronoUnit.CENTURIES)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        for(InfluxData measurement: data){
            logger.info(measurement.toString());
        }
        logger.info("{} Bucket measurements received: {}", userName, data.size());
        return retList;
    }

    /**
     * Load all of a users data from their VFS bucket.
     *
     * @param userName
     * @return
     */
    public List<InfluxData> getAllUserVfsData(String userName) {
        if (this.influxDBClient.getBucketsApi().findBucketByName(userName) == null) {
            return new ArrayList<>();
        }
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        logger.info(queryApi.toString());
        Flux query = Flux.from(userName)
                .range(-1L, ChronoUnit.CENTURIES)
                .pivot()
                .withRowKey(new String[]{"_time", "ods_user"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(query.toString(), InfluxData.class);
        for(InfluxData measurement: data){
            logger.info(measurement.toString());
        }
        logger.info("{} Bucket measurements received: {}", userName, data.size());
        return data;
    }

    public List<InfluxData> getMeasurementsOfJob(Long jobId, String userName){
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        List<InfluxData> retList = new ArrayList<>();
        //check the public bucket
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(-1L, ChronoUnit.CENTURIES)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .filter(Restrictions.and(Restrictions.field().equal(ODS_JOB_ID), Restrictions.value().equal(userName)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        fluxQuery = Flux.from(userName)
                .range(-1L, ChronoUnit.CENTURIES)
                .filter(Restrictions.and(Restrictions.field().equal(ODS_JOB_ID), Restrictions.value().equal(userName)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> allData = queryApi.query(fluxQuery.toString(), InfluxData.class);
        allData.addAll(data);
        for(InfluxData measurement: allData){
            logger.info(measurement.toString());
        }
        logger.info("JobId:{} measurements received: {}", jobId, allData.size());
        return retList;
    }

    public List<InfluxData> jobsByDateGlobalBucket(Date date, String userName){
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(date.toInstant())
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        fluxQuery = Flux.from(userName)
                .range(date.toInstant())
                .filter(Restrictions.and(Restrictions.field().equal(ODS_JOB_ID), Restrictions.value().equal(userName)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");

    }

    public void onboardOdsUser(String username, String password) {
        OnboardingRequest onboardingRequest = new OnboardingRequest();
        onboardingRequest.setBucket(username);
        onboardingRequest.setOrg(this.influxOrg);
        onboardingRequest.setPassword(password);
        onboardingRequest.setUsername(username);
        onboardingRequest.setToken(this.token);
        try {
            OnboardingResponse response = this.influxDBClient.onBoarding(onboardingRequest);
        } catch (UnprocessableEntityException e) {
            logger.error("We already created this user {}", onboardingRequest.getUsername());
        }
    }
}