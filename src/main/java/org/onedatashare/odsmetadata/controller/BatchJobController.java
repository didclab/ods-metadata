package org.onedatashare.odsmetadata.controller;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.onedatashare.odsmetadata.model.BatchJobData;
import org.onedatashare.odsmetadata.repository.BatchJobRepository;
import org.onedatashare.odsmetadata.services.JobService;
import org.onedatashare.odsmetadata.services.QueryingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This controller allows a user to query jobs that they have submitted from CockroachDB
 */
@Slf4j
@RestController
@RequestMapping(value="/api/v1/meta", produces = MediaType.APPLICATION_JSON_VALUE)
public class BatchJobController {
    @Autowired
    JobService jobService;

    private static final String REGEX_PATTERN = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
            + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$"; //this is used to validate that the userId is an email


    /**
     * Returns all the jobs with the corresponding userId
     * This call should be done if you only want the JobIds
     * @param userId
     * @return List of jobIds
     */
    @GetMapping("/user_jobs")
    public List<Long> getUserJobIds(@RequestParam(value="userId") String userId){
        List<Long> userIdList = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        log.info(userId);
        if(validateuserId(userId)) {
            userIdList = jobService.getUserJobIds(userId);
        }
        return userIdList;
    }

    /**
     * This is a bulk API call so if the user wants all information on all their jobs this is the right call
     * @param userId
     * @return A list of all JobStatistic involving a user
     */
    @GetMapping("/all_stats")
    public List<BatchJobData> getAllJobStatisticsOfUser(@RequestParam(value="userId") String userId){
        List<BatchJobData> allJobStatsOfUser = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        log.info(userId);
        if(validateuserId(userId)) {
            allJobStatsOfUser =  jobService.getAllJobStatisticsOfUser(userId);
        }
        return allJobStatsOfUser;

    }

    /**
     * Returns the meta data regarding any one job
     * @param jobId
     * @return
     */
    @GetMapping("/stat")
    public BatchJobData getJobStatistic(@RequestParam(value = "jobId") String jobId){
        log.info(jobId);
        return jobService.getJobStat(jobId);
    }


    /**stats/date
     * @param userId
     * @param date
     * @return
     */
    @GetMapping("/stats/date")
    public BatchJobData getUserJobsByDate(@RequestParam(value="userId") String userId, @RequestParam(value="date")
    @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss.SSS") Date date){
        log.info(date.toString());
        log.info(userId);
        return jobService.getUserJobsByDate(userId,date);
    }

    /**
     * @param userId
     * @param to
     * @param from
     * @return
     */
    @GetMapping("/stats/date/range")
    public List<Long> getUserJobsByDateRange(@RequestParam(value = "userId") String userId,
                                                @RequestParam(value="from")
                                                @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss.SSS") Date from,
                                                @RequestParam(value="to")
                                                @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss.SSS") Date to){

        List<Long> userJobsByDateRange = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        if(validateuserId(userId)) {
            userJobsByDateRange = jobService.getUserJobsByDateRange(userId, from, to);
        }
        return userJobsByDateRange;
    }



    private boolean validateuserId(String userId){
        return Pattern.compile(REGEX_PATTERN)
                .matcher(userId)
                .matches();
    }
}
