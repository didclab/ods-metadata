package org.onedatashare.odsmetadata.controller;

import com.google.common.base.Preconditions;
import org.onedatashare.odsmetadata.model.BadEmailException;
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
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * This controller allows a user to query jobs that they have submitted form CockroachDB
 */
@RestController
@RequestMapping(value = "/api/v1/meta", produces = MediaType.APPLICATION_JSON_VALUE)
public class JobController {

    private static final Logger logger = LoggerFactory.getLogger(JobController.class);

    @Autowired
    QueryingService queryingService;

    @Autowired
    InfluxIOService influxIOService;

    private static final String REGEX_PATTERN = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
            + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$"; //this is used to validate that the userId is an email
    private static final String REGEX = "\\d+";


    /**
     * Returns all the jobs with the corresponding userId
     * This call should be done if you only want the JobIds
     * This should also return Longs and not Integers as jobIds' are longs not ints
     *
     * @param userId
     * @return List of jobIds
     */
    @GetMapping("/user_jobs")
    public List<Integer> getUserJobIds(@RequestParam String userId) {
        ArrayList<Integer> userIdList = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if (validateUserEmail(userId)) {
            userIdList = (ArrayList<@Valid Integer>) queryingService.queryUserJobIds(userId);
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
    public Set<JobStatisticDto> getAllJobStatisticsOfUser(@RequestParam String userId) {
        List<JobStatistic> allJobStatsOfUser = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if (validateUserEmail(userId)) {
            allJobStatsOfUser = queryingService.queryGetAllJobStatisticsOfUser(userId);
        }
        return queryingService.getJobStatisticDtos(allJobStatsOfUser);

    }

    /**
     * Returns the meta data regarding any one job
     *
     * @param jobId
     * @return
     */
    @GetMapping("/stat")
    public Set<JobStatisticDto> getJobStat(@RequestParam String jobId) {
        List<JobStatistic> anyJobStat = Collections.emptyList();
        logger.info(jobId);
        if (jobId.matches(REGEX)) {
            anyJobStat = queryingService.queryGetJobStat(jobId);
        }
        return queryingService.getJobStatisticDtos(anyJobStat);
    }

    /**
     * @param userId
     * @param date
     * @return
     */
    @GetMapping("/stats/date")
    public Set<JobStatisticDto> getUserJobsByDate(@RequestParam String userId,
                                                  @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime date) {
        List<JobStatistic> userJobsBydate = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if (validateUserEmail(userId)) {
            userJobsBydate = queryingService.queryGetUserJobsByDate(userId, date.toInstant(ZoneOffset.UTC));
        }
        return queryingService.getJobStatisticDtos(userJobsBydate);

    }

    /**
     * I think supplying seconds and miliseconds is unecessary so i would change the DateTimeFormat here to be more reasonable
     * <p>
     * This should also return the JobStatisticDTO not a list of integers
     *
     * @param userId
     * @param to
     * @param from
     * @return
     */
    @GetMapping("/stats/date/range")
    public List<Integer> getUserJobsByDateRange(@RequestParam(value = "userId") String userId,
                                                @RequestParam(value = "from")
                                                @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
                                                @RequestParam(value = "to")
                                                @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to) {

        List<Integer> userJobsByDateRange = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        if (validateUserEmail(userId)) {
            userJobsByDateRange = queryingService.queryGetUserJobsByDateRange(userId, from.toInstant(ZoneOffset.UTC), to.toInstant(ZoneOffset.UTC));
        }
        return userJobsByDateRange;
    }

    @GetMapping("/stats/influx/job")
    public List<InfluxData> getJobMeasurements(@RequestParam String userEmail, @RequestParam Long jobId) throws BadEmailException {
        if (!validateUserEmail(userEmail)) {
            throw new BadEmailException();
        }
        List<InfluxData> data = influxIOService.getUserJobInfluxData(jobId, userEmail);
        data.addAll(influxIOService.getUserJobVfsBucketData(jobId, userEmail));
        return data;
    }

    @GetMapping("/stats/influx/user")
    public List<InfluxData> getAllUserJobs(@RequestParam String userEmail) throws BadEmailException {
        if (!validateUserEmail(userEmail)) {
            throw new BadEmailException();
        }
        List<InfluxData> data = influxIOService.getAllUserGlobalData(userEmail);
        data.addAll(influxIOService.getAllUserVfsData(userEmail));
        return data;
    }

    @GetMapping("/stats/influx/job/range")
    public List<InfluxData> getMeasurementsByDateRange(@RequestParam String userEmail,
                                                       @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start,
                                                       @RequestParam("end") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime end) throws BadEmailException {
        if (!validateUserEmail(userEmail)) {
            throw new BadEmailException();
        }
        List<InfluxData> data = influxIOService.vfsMeasurementsByDates(start.toInstant(ZoneOffset.UTC), end.toInstant(ZoneOffset.UTC), userEmail);
        data.addAll(influxIOService.globalMeasurementsByDates(start.toInstant(ZoneOffset.UTC), end.toInstant(ZoneOffset.UTC), userEmail));
        return data;
    }

    @GetMapping("/stats/influx/job/date")
    public List<InfluxData> getMeasurementsByDate(@RequestParam String userEmail, @RequestParam("start") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime start) throws BadEmailException {
        if (!validateUserEmail(userEmail)) {
            throw new BadEmailException();
        }
        List<InfluxData> data = influxIOService.jobsByDateGlobalBucket(start.toInstant(ZoneOffset.UTC), userEmail);
        data.addAll(influxIOService.jobsByDateVfsBucket(start.toInstant(ZoneOffset.UTC), userEmail));
        return data;
    }

    private boolean validateUserEmail(String userId) {
        return Pattern.compile(REGEX_PATTERN)
                .matcher(userId)
                .matches();
    }
}