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

import javax.annotation.PostConstruct;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

@Service
public class InfluxIOService {

    Logger logger = LoggerFactory.getLogger(InfluxIOService.class);

    private InfluxDBClient influxDBClient;

    private final String ODS_USER = "ods_user";
    private final String ODS_JOB_ID = "jobId";
    private final String MEASUREMENT = "transfer_data";

    private final String defaultBucket = "OdsTransferNodes";

    @Value("${influxdb.org}")
    private String influxOrg;

    @Value("${influxdb.token}")
    private String token;

    @Value("${influxdb.measurement}")
    private String measurement;

    private final String filterByJobId = "filter(fn: (r) => r.jobId == %s)";
    private QueryApi queryApi;

    public InfluxIOService(InfluxDBClient influxDBClient) {
        this.influxDBClient = influxDBClient;
    }

    @PostConstruct
    public void postConstruct(){
        this.queryApi = this.influxDBClient.getQueryApi();
    }

    /**
     * Load all of a users data that ran through AWS
     *
     * @param userName
     * @return
     */
    public List<InfluxData> getAllUserGlobalData(String userName) {
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(-1L, ChronoUnit.CENTURIES)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
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
        Restrictions restrictions = Restrictions.and(
                Restrictions.measurement().equal("transfer_data"),
                Restrictions.tag("ods_user").equal(userName)
        );
        Flux flux = Flux.from(userName)
                .range(-1L, ChronoUnit.DAYS)
                .filter(restrictions)
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
        String fluxStr = flux.toString();
        logger.info("Vfs Flux Query = {}", fluxStr);
        return  queryApi.query(fluxStr, InfluxData.class);
    }

    /**
     * Public bucket data
     *
     * @param jobId
     * @param userName
     * @return
     */
    public List<InfluxData> getUserJobInfluxData(Long jobId, String userName) {
        Restrictions globalRestrictions = Restrictions.and(
                Restrictions.tag(ODS_USER).equal(userName),
                Restrictions.tag(ODS_JOB_ID).equal(jobId)
        );
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(-1L, ChronoUnit.CENTURIES)
                .filter(globalRestrictions)
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("Global Job {}: size {}", jobId, data.size());
        return data;
    }

    public List<InfluxData> queryVfsBucketWithJobId(Long jobId, String userName){
        if (this.influxDBClient.getBucketsApi().findBucketByName(userName) == null) {
            return new ArrayList<>();
        }
        Restrictions restrictions = Restrictions.and(
                Restrictions.measurement().equal("transfer_data"),
                Restrictions.tag("jobId").equal(String.valueOf(jobId)),
                Restrictions.tag("ods_user").equal(userName)
        );
        Flux flux = Flux.from(userName)
                .range(-1L, ChronoUnit.DAYS)
                .filter(restrictions)
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
        String fluxStr = flux.toString();
        logger.info("Vfs Flux Query = {}", fluxStr);
        List<InfluxData> data = queryApi.query(fluxStr, InfluxData.class);
        return data;
    }

    public List<InfluxData> globalMeasurementsByDates(Instant start, Instant end, String userEmail) {
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(start, end)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userEmail)))
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("VFS Jobs start:{} end:{}: size {}", start, end, data.size());
        return data;
    }

    public List<InfluxData> vfsMeasurementsByDates(Instant start, Instant end, String userEmail) {
        if (this.influxDBClient.getBucketsApi().findBucketByName(userEmail) == null) {
            return new ArrayList<>();
        }
        logger.info("Start: {} End:{}", start, end);
        Flux fluxQuery = Flux.from(userEmail)
                .range(start, end)
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("VFS Jobs start:{} end:{}: size {}", start, end, data.size());
        return data;
    }


    public List<InfluxData> jobsByDateGlobalBucket(Instant instant, String userName) {
        Flux fluxQuery = Flux.from(defaultBucket)
                .range(instant)
                .filter(Restrictions.and(Restrictions.tag(ODS_USER).equal(userName)))
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("Global Jobs By Date: size {}", data.size());
        return data;
    }

    public List<InfluxData> jobsByDateVfsBucket(Instant instant, String userName) {
        if (this.influxDBClient.getBucketsApi().findBucketByName(userName) == null) {
            return new ArrayList<>();
        }
        Flux fluxQuery = Flux.from(userName)
                .range(instant)
                .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
        List<InfluxData> data = queryApi.query(fluxQuery.toString(), InfluxData.class);
        logger.info("VFS Jobs By Date: size {}", data.size());
        return data;
    }

    public List<InfluxData> monitorMeasurement(String userName, Long jobId) {
        //check if the vfs bucket exists then we check that one first for the jobId running there.
        if (this.influxDBClient.getBucketsApi().findBucketByName(userName) != null) {
            Restrictions vfsRestrictions = Restrictions.and(
                    Restrictions.measurement().equal(MEASUREMENT),
                    Restrictions.tag(ODS_JOB_ID).equal(String.valueOf(jobId))
            );
            Flux fluxQuery = Flux.from(userName)
                    .range(-60L, ChronoUnit.SECONDS)
                    .filter(vfsRestrictions)
                    .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
            return queryApi.query(fluxQuery.toString(), InfluxData.class);
        } else {
            //query global bucket
            Restrictions globalRestrictions = Restrictions.and(
                    Restrictions.measurement().equal(MEASUREMENT),
                    Restrictions.tag(ODS_JOB_ID).equal(String.valueOf(jobId)),
                    Restrictions.tag(ODS_USER).equal(userName)
            );
            //otherwise we check the global bucket for these values.
            Flux fluxQuery = Flux.from(defaultBucket)
                    .range(-60L, ChronoUnit.SECONDS)
                    .filter(globalRestrictions)
                    .pivot(new String[]{"_time"}, new String[]{"_field"}, "_value");
            return queryApi.query(fluxQuery.toString(), InfluxData.class);
        }
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