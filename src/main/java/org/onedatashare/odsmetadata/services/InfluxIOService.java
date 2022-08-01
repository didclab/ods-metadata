package org.onedatashare.odsmetadata.services;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.QueryApi;
import com.influxdb.client.domain.OnboardingRequest;
import com.influxdb.client.domain.OnboardingResponse;
import com.influxdb.exceptions.UnprocessableEntityException;
import com.influxdb.query.dsl.Flux;
import com.influxdb.query.dsl.functions.restriction.Restrictions;
import org.onedatashare.odsmetadata.model.InfluxData;
import org.onedatashare.odsmetadata.model.JobStatistic;
import org.onedatashare.odsmetadata.model.CdbInfluxData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
public class InfluxIOService {

    Logger logger = LoggerFactory.getLogger(InfluxIOService.class);

    @Autowired
    private InfluxDBClient influxDBClient;

    @Autowired
    QueryingService queryingService;


    private final String ODS_USER = "ods_user";
    private final String ODS_JOB_ID = "jobId";

    private final String defaultBucket = "OdsTransferNodes";

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
        String f = "filter(fn: (r) => r.jobId == %s)";
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(-1L, ChronoUnit.CENTURIES)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .pivot()
                .withRowKey(new String[]{"_time"})
                .withColumnKey(new String[]{"_field"})
                .withValueColumn("_value")
                .expression(String.format(f, jobId));
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("Global Job {}: size {}", jobId, data.size());
        return data;
    }

    public List<InfluxData> getUserJobVfsBucketData(Long jobId, String userName) {
        if (this.influxDBClient.getBucketsApi().findBucketByName(userName) == null) {
            return new ArrayList<>();
        }
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        String f = "filter(fn: (r) => r.jobId == %s)";
        Flux fluxQuery = Flux.from(userName)
                .range(-1L, ChronoUnit.CENTURIES)
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value")
                .expression(String.format(f, jobId));

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

    public List<CdbInfluxData> monitorPublicBucketJobs(String userEmail, Instant startTime, List<Long> jobIds) {
        String f = "filter(fn: (r) => r.jobId == %s)";
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        return jobIds.parallelStream().map(jobId -> {
            Flux fluxQuery = Flux.from(defaultBucket)
                    .range(startTime)
                    .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userEmail)))
                    .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value")
                    .expression(String.format(f, jobId));

            List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
            JobStatistic jobStatistic = queryingService.queryGetJobStat(String.valueOf(jobId)).get(0);
            CdbInfluxData cdbInfluxData = new CdbInfluxData();
            cdbInfluxData.setJobStatistic(jobStatistic);
            cdbInfluxData.setMeasurements(data);
            return cdbInfluxData;
        }).collect(Collectors.toList());
    }

    public List<CdbInfluxData> monitorPrivateBucketJobs(String userEmail, Instant startTime, List<Long> jobIds) {
        String f = "filter(fn: (r) => r.jobId == %s)";
        logger.info(userEmail, startTime, jobIds.toString());
        QueryApi queryApi = this.influxDBClient.getQueryApi();
        return jobIds.parallelStream().map(jobId -> {
            Flux fluxQuery = Flux.from(userEmail)
                    .range(startTime)
                    .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value")
                    .expression(String.format(f, jobId));
            List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
            CdbInfluxData cdbInfluxData = new CdbInfluxData();
            cdbInfluxData.setMeasurements(data);
            JobStatistic jobStatistic = queryingService.queryGetJobStat(String.valueOf(jobId)).get(0);
            cdbInfluxData.setJobStatistic(jobStatistic);
            return cdbInfluxData;
        }).collect(Collectors.toList());
    }
}