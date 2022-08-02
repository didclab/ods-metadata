package org.onedatashare.odsmetadata.controller;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.onedatashare.odsmetadata.model.BatchJobData;
import org.onedatashare.odsmetadata.model.InfluxData;
import org.onedatashare.odsmetadata.model.MonitorData;
import org.onedatashare.odsmetadata.services.InfluxIOService;
import org.onedatashare.odsmetadata.services.JobService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This controller allows a user to query jobs that they have submitted from CockroachDB
 */
@Slf4j
@RestController
@RequestMapping(value = "/api/v1/meta", produces = MediaType.APPLICATION_JSON_VALUE)
public class BatchJobController {
    @Autowired
    JobService jobService;

    @Autowired
    InfluxIOService influxIOService;

    private static final String REGEX_PATTERN = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
            + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$"; //this is used to validate that the userEmail is an email

    private static final String REGEX = "\\d+";

    /**
     * Returns all the jobs with the corresponding userEmail
     * This call should be done if you only want the JobIds
     *
     * @param userEmail
     * @return List of jobIds
     */
    @GetMapping("/user_jobs")
    public List<Long> getUserJobIds(@RequestParam String userEmail) {
        List<Long> userEmailList = new ArrayList<>();
        Preconditions.checkNotNull(userEmail);
        log.info(userEmail);
        if (validateUserEmail(userEmail)) {
            userEmailList = jobService.getUserJobIds(userEmail);
        }
        return userEmailList;
    }

    /**
     * This is a bulk API call so if the user wants all information on all their jobs this is the right call
     *
     * @param userEmail
     * @return A list of all JobStatistic involving a user
     */
    @GetMapping("/all_stats")
    public List<BatchJobData> getAllJobStatisticsOfUser(@RequestParam String userEmail) {
        List<BatchJobData> allJobStatsOfUser = new ArrayList<>();
        Preconditions.checkNotNull(userEmail);
        log.info(userEmail);
        if (validateUserEmail(userEmail)) {
            allJobStatsOfUser = jobService.getAllJobStatisticsOfUser(userEmail);
        }
        return allJobStatsOfUser;

    }

    /**
     * Returns the meta data regarding any one job
     *
     * @param jobId
     * @return
     */
    @GetMapping("/stat")
    public BatchJobData getJobStatistic(@RequestParam Long jobId) {
        BatchJobData batchJobData = new BatchJobData();
        if (String.valueOf(jobId).matches(REGEX)) {
            batchJobData = jobService.getJobStat(jobId);
        }
        return batchJobData;
    }


    /**
     * @param userEmail
     * @param date
     * @return
     */
    @GetMapping("/stat/date")
    public BatchJobData getUserJobsByDate(@RequestParam String userEmail, @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime date) {
        log.info(userEmail);
        log.info(date.toString());
        BatchJobData batchJobData = new BatchJobData();
        Preconditions.checkNotNull(userEmail);
        if (validateUserEmail(userEmail)) {
            batchJobData = jobService.getUserJobsByDate(userEmail, date.toInstant(ZoneOffset.UTC));
        }
        return batchJobData;
    }

    /**
     * @param userEmail
     * @param to
     * @param from
     * @return
     */
    @GetMapping("/stats/date/range")
    public List<BatchJobData> getUserJobsByDateRange(@RequestParam String userEmail,
                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        List<BatchJobData> userJobsByDateRange = new ArrayList<>();
        Preconditions.checkNotNull(userEmail);
        if (validateUserEmail(userEmail)) {
            userJobsByDateRange = jobService.getUserJobsByDateRange(userEmail, from.toInstant(ZoneOffset.UTC), to.toInstant(ZoneOffset.UTC));
        }
        return userJobsByDateRange;
    }


    @GetMapping("/stats/influx/job")
    public List<InfluxData> getJobMeasurements(@RequestParam String userEmail, @RequestParam Long jobId) {
        log.info(userEmail);
        log.info(jobId.toString());
        List<InfluxData> data = influxIOService.getUserJobInfluxData(jobId, userEmail);
        data.addAll(influxIOService.getUserJobVfsBucketData(jobId, userEmail));
        return data;
    }

    @GetMapping("/stats/influx/job/range")
    public List<InfluxData> getMeasurementsByDateRange(@RequestParam String userEmail,
                                                       @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
                                                       @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end) {
        List<InfluxData> data = influxIOService.vfsMeasurementsByDates(start.toInstant(ZoneOffset.UTC), end.toInstant(ZoneOffset.UTC), userEmail);
        data.addAll(influxIOService.globalMeasurementsByDates(start.toInstant(ZoneOffset.UTC), end.toInstant(ZoneOffset.UTC), userEmail));
        return data;
    }

    @GetMapping("/stats/influx/job/date")
    public List<InfluxData> getMeasurementsByDate(@RequestParam String userEmail, @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start) {
        List<InfluxData> data = influxIOService.jobsByDateGlobalBucket(start.toInstant(ZoneOffset.UTC), userEmail);
        data.addAll(influxIOService.jobsByDateVfsBucket(start.toInstant(ZoneOffset.UTC), userEmail));
        return data;
    }

    @GetMapping("/stats/influx/user")
    public List<InfluxData> getAllUserJobs(@RequestParam String userEmail) {
        List<InfluxData> data = influxIOService.getAllUserGlobalData(userEmail);
        data.addAll(influxIOService.getAllUserVfsData(userEmail));
        return data;
    }

    private boolean validateUserEmail(String userEmail) {
        return Pattern.compile(REGEX_PATTERN)
                .matcher(userEmail)
                .matches();
    }

    @GetMapping("/monitor")
    public MonitorData monitor(@RequestParam String userEmail, @RequestParam(value = "jobIds") Long[] jobIds) {
        MonitorData monitorData = new MonitorData();
        List<BatchJobData> jobData = new ArrayList<>();
        List<InfluxData> measurementData = new ArrayList<>();
        for (Long jobId : jobIds) {
            measurementData.addAll(influxIOService.monitorMeasurement(userEmail, jobId));
            jobData.add(jobService.getJobStat(jobId));
        }
        monitorData.setJobData(jobData);
        monitorData.setMeasurementData(measurementData);
        return monitorData;
    }
}
