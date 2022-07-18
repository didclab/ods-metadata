package org.onedatashare.odsmetadata.controller;

import com.google.common.base.Preconditions;
import org.onedatashare.odsmetadata.model.InfluxData;
import org.onedatashare.odsmetadata.model.JobStatistic;
import org.onedatashare.odsmetadata.model.JobStatisticDto;
import org.onedatashare.odsmetadata.services.InfluxIOService;
import org.onedatashare.odsmetadata.services.QueryingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.Valid;
import java.util.*;
import java.util.regex.Pattern;

/**
 * This controller allows a user to query jobs that they have submitted form CockroachDB
 */
@RestController
@RequestMapping(value = "/api/v1/meta", produces = MediaType.APPLICATION_JSON_VALUE)
public class JobController {

    @Autowired
    QueryingService queryingService;

    @Autowired
    InfluxIOService influxIOService;

    private static final Logger logger = LoggerFactory.getLogger(JobController.class);
    private static final String REGEX_PATTERN = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
            + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$"; //this is used to validate that the userId is an email
    private static final String REGEX = "\\d+";


    /**
     * Returns all the jobs with the corresponding userId
     * This call should be done if you only want the JobIds
     *
     * @param userId
     * @return List of jobIds
     */
    @GetMapping("/user_jobs")
    public List<Long> getUserJobIds(@RequestParam(value = "userId") String userId) {
        ArrayList<Long> userIdList = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if (validateuserId(userId)) {
            userIdList = (ArrayList<@Valid Long>) queryingService.queryUserJobIds(userId);
        }
        return userIdList;
    }

    /**
     * This is a bulk API call so if the user wants all information on all their jobs this is the right call
     *
     * @param userId
     * @return A list of all JobStatistic involving a user
     */
    @GetMapping("/all_stats")
    public Set<JobStatisticDto> getAllJobStatisticsOfUser(@RequestParam(value = "userId") String userId) {
        List<JobStatistic> allJobStatsOfUser = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        if (validateuserId(userId)) {
            allJobStatsOfUser = queryingService.queryGetAllJobStatisticsOfUser(userId);
        }
        Set<JobStatisticDto> jobStatisticDtos = queryingService.getJobStatisticDtos(allJobStatsOfUser); //N jobs for user
        List<InfluxData> publicBucketData = influxIOService.getAllUserInfluxData(userId);
        List<InfluxData> userVfsBucketData = influxIOService.getAllUserVfsData(userId);
        publicBucketData.addAll(userVfsBucketData);
        aggCdbInfluxData(publicBucketData, jobStatisticDtos);
        return jobStatisticDtos;

    }

    /**
     * Returns the meta data regarding any one job
     *
     * @param jobId
     * @return
     */
    @GetMapping("/stat")
    public Set<JobStatisticDto> getJobStat(@RequestParam(value = "jobId") String jobId, @RequestParam String userEmail) {
        List<JobStatistic> anyJobStat = Collections.emptyList();
        logger.info(jobId);
        if (jobId.matches(REGEX)) {
            anyJobStat = queryingService.queryGetJobStat(jobId);
        }
        Set<JobStatisticDto> jobStatisticDtos = queryingService.getJobStatisticDtos(anyJobStat); //N jobs for user
        if(jobStatisticDtos.size() > 0){
            Object[] data = jobStatisticDtos.toArray();
            JobStatisticDto one = (JobStatisticDto) data[0];
            List<InfluxData> influxData = influxIOService.getMeasurementsOfJob(one.getJobId(), userEmail);
            one.setJobMeasurements(influxData);
            return Set.of(one);
        }
        return jobStatisticDtos;
    }

    /**
     * @param userId
     * @param date
     * @return
     */
    @GetMapping("/stats/date")
    public Set<JobStatisticDto> getUserJobsByDate(@RequestParam(value = "userId") String userId, @RequestParam(value = "date")
    @DateTimeFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS") Date date) {

        List<JobStatistic> userJobsBydate = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if (validateuserId(userId)) {
            userJobsBydate = queryingService.queryGetUserJobsByDate(userId, date);
        }
        Set<JobStatisticDto> cdbStats = queryingService.getJobStatisticDtos(userJobsBydate);
        return cdbStats;

    }

    private boolean validateuserId(String userId) {
        return Pattern.compile(REGEX_PATTERN)
                .matcher(userId)
                .matches();
    }

    //Should probably use merge-sort here as this is O(N*M)
    private void aggCdbInfluxData(List<InfluxData> measurements, Set<JobStatisticDto> jobStatisticDtos) {
        for (JobStatisticDto jobStatisticDto : jobStatisticDtos) {//O(N)
            for (int i = 0; i < measurements.size(); i++) { // O(M) as each job has M measurements
                InfluxData measurement = measurements.get(i);
                if (jobStatisticDto.getJobId() == measurement.getJobId()) {
                    jobStatisticDto.getJobMeasurements().add(measurement);
                }
            }
        }
    }
}