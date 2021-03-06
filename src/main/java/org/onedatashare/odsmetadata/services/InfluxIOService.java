package org.onedatashare.odsmetadata.services;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.OnboardingRequest;
import com.influxdb.client.domain.OnboardingResponse;
import com.influxdb.exceptions.UnprocessableEntityException;
import com.influxdb.query.dsl.Flux;
import com.influxdb.query.dsl.functions.restriction.Restrictions;
import org.onedatashare.odsmetadata.model.InfluxData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@Service
public class InfluxIOService {

    Logger logger = LoggerFactory.getLogger(InfluxIOService.class);

    @Autowired
    private InfluxDBClient influxDBClient;

    private final String ODS_USER = "ods_user";
    private final String ODS_JOB_ID = "jobId";

    private final String defaultBucket = "OdsTransferNodes";

    @Value("influxdb.org")
    private String influxOrg;

    @Value("influxdb.token")
    private String token;

    @Value("influxdb.measurement")
    private String measurement;

    private final String filterByJobId = "filter(fn: (r) => r.jobId == %s)";

    /**
     * Load all of a users data that ran through AWS
     *
     * @param userName
     * @return
     */
    public List<InfluxData> getAllUserGlobalData(String userName) {
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(-1L, ChronoUnit.CENTURIES)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("Global All User {}: size {}", userName, data.size());
        return data;
    }

    /**
     * Load all of a users data from their VFS(name of the bucket as of this writing is the users email) bucket.
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
        Flux fluxQuery = Flux.from(userName)
                .range(-1L, ChronoUnit.CENTURIES)
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("VFS All User {}: size {}", userName, data.size());
        return data;
    }

    /**
     * Public bucket data
     *
     * @param jobId
     * @param userName
     * @return
     */
    public List<InfluxData> getUserJobInfluxData(Long jobId, String userName) {
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(-1L, ChronoUnit.CENTURIES)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value")
                .expression(String.format(filterByJobId, jobId));
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("Global Job {}: size {}", jobId, data.size());
        return data;
    }

    public List<InfluxData> getUserJobVfsBucketData(Long jobId, String userName) {
        if (this.influxDBClient.getBucketsApi().findBucketByName(userName) == null) {
            return new ArrayList<>();
        }
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        Flux fluxQuery = Flux.from(userName)
                .range(-1L, ChronoUnit.CENTURIES)
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value")
                .expression(String.format(filterByJobId, jobId));

        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("VFS Job {}: size {}", jobId, data.size());
        return data;
    }

    public List<InfluxData> globalMeasurementsByDates(Instant start, Instant end, String userEmail) {
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(start, end)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userEmail)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("VFS Jobs start:{} end:{}: size {}", start, end, data.size());
        return data;
    }

    public List<InfluxData> vfsMeasurementsByDates(Instant start, Instant end, String userEmail) {
        if (this.influxDBClient.getBucketsApi().findBucketByName(userEmail) == null) {
            return new ArrayList<>();
        }
        logger.info("Start: {} End:{}", start, end);
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        Flux fluxQuery = Flux.from(userEmail)
                .range(start, end)
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("VFS Jobs start:{} end:{}: size {}", start, end, data.size());
        return data;
    }


    public List<InfluxData> jobsByDateGlobalBucket(Instant instant, String userName) {
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(instant)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("Global Jobs By Date: size {}", data.size());
        return data;
    }

    public List<InfluxData> jobsByDateVfsBucket(Instant instant, String userName) {
        if (this.influxDBClient.getBucketsApi().findBucketByName(userName) == null) {
            return new ArrayList<>();
        }
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        Flux fluxQuery = Flux.from(userName)
                .range(instant)
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("VFS Jobs By Date: size {}", data.size());
        return data;
    }

    public List<InfluxData> monitorMeasurement(String userName, Long jobId) {
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        if (this.influxDBClient.getBucketsApi().findBucketByName(userName) != null) {
            Flux fluxQuery = Flux.from(userName)
                    .range(-30L, ChronoUnit.SECONDS)
                    .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value")
                    .expression(String.format(filterByJobId, jobId));
            List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
            if (data.size() > 0) {
                return data;
            }
        }
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(-30L, ChronoUnit.SECONDS)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value")
                .expression(String.format(filterByJobId, jobId));
        return queryApi.query(fluxQuery.toString(), InfluxData.class);
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