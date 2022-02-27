package org.onedatashare.odsmetadata.controller;

import com.google.common.base.Preconditions;
import org.onedatashare.odsmetadata.model.JobStatistic;
import org.onedatashare.odsmetadata.services.QueryingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This controller allows a user to query jobs that they have submitted form CockroachDB
 */
@RestController
@RequestMapping(value="/api/v1/meta", produces = MediaType.APPLICATION_JSON_VALUE)
public class JobController {

    @Autowired
    QueryingService queryingService;
    private static final Logger logger = LoggerFactory.getLogger(JobController.class);
    private static final String REGEX_PATTERN = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
            + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$"; //this is used to validate that the userId is an email
    /**
     * Returns all the jobs with the corresponding userId
     * This call should be done if you only want the JobIds
     * @param userId
     * @return List of jobIds
     */
    @GetMapping("/user_jobs")
    public List<Integer> getUserJobIds(@RequestParam(value="userId") String userId){
        ArrayList <Integer> userIdList = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if(validateuserId(userId)) {
            userIdList = (ArrayList<@Valid Integer>) queryingService.queryUserJobIds(userId);
        }
        return userIdList;
    }

    /**
     * This is a bulk API call so if the user wants all information on all their jobs this is the right call
     * @param userId
     * @return A list of all JobStatistic involving a user
     */
    @GetMapping("/all_stats")
    public List<JobStatistic> getAllJobStatisticsOfUser(@RequestParam(value="userId") String userId){
        List <JobStatistic> allJobStatsOfUser = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if(validateuserId(userId)) {
            allJobStatsOfUser =  queryingService.queryGetAllJobStatisticsOfUser(userId);
        }
        return allJobStatsOfUser;
    }

    /**
     * Returns the meta data regarding any one job
     * @param jobId
     * @return
     */
    @GetMapping("/stat")
    public JobStatistic getJobStat(@RequestParam(value = "jobId") String jobId){
        JobStatistic anyJobStat = null;
        String regex = "\\d+";
        logger.info(jobId);
        if(jobId.matches(regex)) {
            anyJobStat = queryingService.queryGetJobStat(jobId);
        }
        return anyJobStat;
    }

    /**
     * @param userId
     * @param date
     * @return
     */
    @GetMapping("/stats/date")
    public List <JobStatistic> getUserJobsByDate(@RequestParam(value="userId") String userId, @RequestParam(value="date")
                                                 @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss.SSS") Date date){
        List<JobStatistic> userJobsBydate = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if(validateuserId(userId)) {
            userJobsBydate = queryingService.queryGetUserJobsByDate(userId, date);
        }
        return userJobsBydate;

    }

    /**
     * @param userId
     * @param to
     * @param from
     * @return
     */
    @GetMapping("/stats/date/range")
    public List<Integer> getUserJobsByDateRange(@RequestParam(value = "userId") String userId,
                                                      @RequestParam(value="from")
                                                      @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss.SSS") Date from,
                                                      @RequestParam(value="to")
                                                          @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss.SSS") Date to){

        List<Integer> userJobsByDateRange = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        if(validateuserId(userId)) {
            userJobsByDateRange = queryingService.queryGetUserJobsByDateRange(userId, from, to);
        }
        return userJobsByDateRange;
    }

    public boolean validateuserId(String userId){
        return Pattern.compile(REGEX_PATTERN)
                .matcher(userId)
                .matches();
    }
}
