package org.onedatashare.odsmetadata.controller;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.onedatashare.odsmetadata.model.BatchJobData;
import org.onedatashare.odsmetadata.model.InfluxData;
import org.onedatashare.odsmetadata.model.MonitorData;
import org.onedatashare.odsmetadata.model.TransferSummary;
import org.onedatashare.odsmetadata.services.InfluxIOService;
import org.onedatashare.odsmetadata.services.JobService;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
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
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * This controller allows a user to query jobs that they have submitted from CockroachDB
 */
@Slf4j
@RestController
@RequestMapping(value = "/api/v1/meta", produces = MediaType.APPLICATION_JSON_VALUE)
public class BatchJobController {
    JobService jobService;

    InfluxIOService influxIOService;

    public BatchJobController(JobService jobService, InfluxIOService influxIOService) {
        this.jobService = jobService;
        this.influxIOService = influxIOService;
    }

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

    @GetMapping("/uuids")
    public List<UUID> getUserUuids(@RequestParam String userEmail) {
        List<UUID> userUuids = new ArrayList<>();
        Preconditions.checkNotNull(userEmail);
        if (validateUserEmail(userEmail)) {
            userUuids = jobService.getUserUuids(userEmail);
        }
        return userUuids;
    }

    @GetMapping("/job/uuid")
    public List<BatchJobData> getBatchJobByUuid(@RequestParam UUID jobUuid) {
        List<BatchJobData> jobDataList = new ArrayList<>();
        jobDataList = jobService.getBatchDataFromUuids(jobUuid);
        return jobDataList;
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

    @GetMapping("/stats/jobIds")
    public List<BatchJobData> getListOfJobStatsFromIds(@RequestParam String userEmail, @RequestParam List<Long> jobIds) {
        List<BatchJobData> jobStats = new ArrayList<>();
        Preconditions.checkNotNull(userEmail);
        if (validateUserEmail(userEmail)) {
            for (Long jobId : jobIds) {
                jobStats.add(jobService.getJobStat(jobId));
            }
        }
        return jobStats;
    }

    @GetMapping("/stat/page")
    public Page<BatchJobData> getPageJobStats(@RequestParam String userEmail, Pageable pageable) {
        Preconditions.checkNotNull(userEmail);
        Preconditions.checkNotNull(pageable);
        return jobService.getAllJobStatisticsOfUser(userEmail, pageable);
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

    @GetMapping("/deletejob")
    public void deleteJob(@RequestParam Long jobId) {
        log.info("Deleting job:{}",jobId.toString());
        jobService.deleteJob(jobId);
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

    /**
     * @param userEmail
     * @param to
     * @param from
     * @return
     */
    @GetMapping("/stats/page/date/range")
    public Page<BatchJobData> getUserJobsByDateRange(@RequestParam String userEmail,
                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
                                                     Pageable pageable) {

        List<BatchJobData> userJobsByDateRange = new ArrayList<>();
        Page<BatchJobData> ret = new PageImpl<>(userJobsByDateRange);
        Preconditions.checkNotNull(userEmail);
        if (validateUserEmail(userEmail)) {
            ret = jobService.getUserJobsByDateRange(userEmail, from.toInstant(ZoneOffset.UTC), to.toInstant(ZoneOffset.UTC), pageable);
        }
        return ret;
    }


    @GetMapping("/stats/influx/job")
    public List<InfluxData> getJobMeasurements(@RequestParam String userEmail, @RequestParam Long jobId) {
        log.info(userEmail);
        log.info(jobId.toString());
        List<InfluxData> data = influxIOService.getUserJobInfluxData(jobId, userEmail);
        data.addAll(influxIOService.queryVfsBucketWithJobId(jobId, userEmail));
        return data;
    }

    @GetMapping("/stats/influx/uuid")
    public List<InfluxData> getJobMeasurementsUuid(@RequestParam String userEmail, @RequestParam UUID jobUuid) {
        List<InfluxData> data = influxIOService.getJobViaUuid(userEmail, jobUuid);
        return data;
    }

    @GetMapping("/stats/influx/transfer/node")
    public List<InfluxData> getJobMeasurementsUniversal(@RequestParam String userEmail, @RequestParam String appId, @RequestParam Long jobId, @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start) {
        return influxIOService.getMeasurementsPerNode(userEmail, appId, jobId, start);
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
    public MonitorData monitor(@RequestParam String userEmail, @RequestParam(value = "jobId") Long jobId) {
        MonitorData monitorData = new MonitorData();
        log.info("UserName: {} and jobId:{}", userEmail, jobId);
        List<InfluxData> measurementData = influxIOService.monitorMeasurement(userEmail, jobId);
        BatchJobData jobData = jobService.getJobStat(jobId);
        monitorData.setJobData(jobData);
        monitorData.setMeasurementData(measurementData);
        return monitorData;
    }

    @GetMapping("/summary")
    public TransferSummary jobMonitor(@RequestParam String userEmail, @RequestParam UUID jobUuid) {
        List<InfluxData> measurementData = influxIOService.getJobViaUuid(userEmail, jobUuid);
        TransferSummary summary = new TransferSummary();
        summary.updateSummary(measurementData);
        List<BatchJobData> jobData = jobService.getBatchDataFromUuids(jobUuid);
        summary.setTransferStatus(jobData.get(0).getStatus());
        return summary;
    }
}
