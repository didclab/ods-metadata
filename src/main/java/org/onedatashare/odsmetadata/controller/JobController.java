package org.onedatashare.odsmetadata.controller;

import com.google.common.base.Preconditions;
import org.onedatashare.odsmetadata.model.JobStatistics;
import org.onedatashare.odsmetadata.services.QueryingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import javax.validation.Valid;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This controller allows a user to query jobs that they have submitted form CockroachDB
 */
@RestController
@RequestMapping("/api/v1/job")
public class JobController {

    @Autowired
    QueryingService queryingService = new QueryingService();
    private static final Logger logger = LoggerFactory.getLogger(JobController.class);
    private static final String REGEX_PATTERN = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
            + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$";
    /**
     * Returns all the jobs with the corresponding userId
     * This call should be done if you only want the JobIds
     * @param userId
     * @return List of jobIds
     */
    @GetMapping("/{userId}")
    public String getUserJobIds(@PathVariable String userId){
        ArrayList <String> userIdList = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        if(validateuserId(userId)) {
            userIdList = (ArrayList<@Valid String>) queryingService.queryUserJobIds(userId);
        }
        else
            logger.info("Invalid User Id"+userId);
        return userIdList.toString();
    }

    /**
     * This is a bulk API call so if the user wants all information on all their jobs this is the right call
     * @param userId
     * @return A list of all JobStatistics involving a user
     */
    @GetMapping("/stats/{userId}")
    public List<JobStatistics> getAllJobStatisticsOfUser(@PathVariable String userId){
        List <JobStatistics> allJobStatsOfUser = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        if(validateuserId(userId)) {
            allJobStatsOfUser = (ArrayList<@Valid JobStatistics>) queryingService.queryGetAllJobStatisticsOfUser(userId);
        }
        else
            logger.info("Invalid User Id"+userId);
        return allJobStatsOfUser;
    }

    /**
     * Returns the meta data regarding any one job
     * @param jobId
     * @return
     */
    @GetMapping("/stats/{jobId}")
    public JobStatistics getJobStat(@PathVariable String jobId){
        List <JobStatistics> anyJobStat = new ArrayList<>();
        String regex = "\\d+";
        boolean flag = jobId.matches(regex);
        if(flag==true) {
            anyJobStat = (ArrayList<@Valid JobStatistics>) queryingService.queryGetJobStat(jobId);
        }
        return (JobStatistics) anyJobStat;
    }

    /**
     * @param userId
     * @param date
     * @return
     */
    @GetMapping("/stats/{userId}/date")
    public List<JobStatistics> getUserJobsByDate(@PathVariable String userId, @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss") Date date){
        List <JobStatistics> userJobsBydate = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        /*
        to add validation for date
         */
        if(validateuserId(userId)) {
            userJobsBydate = (ArrayList<@Valid JobStatistics>) queryingService.queryGetUserJobsByDate(userId, date);
        }
        else
            logger.info("Invalid User Id"+userId);
        return (List<JobStatistics>) userJobsBydate;
    }

    /**
     * @param userId
     * @param to
     * @param from
     * @return
     */
    @GetMapping("/stats/{userId}/date/range")
    public List<JobStatistics> getUserJobsByDateRange(@PathVariable String userId,
                                                      @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss") Date to,
                                                      @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss") Date from){

        List <JobStatistics> userJobsByDateRange = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        if(validateuserId(userId)) {
            userJobsByDateRange = (ArrayList<@Valid JobStatistics>) queryingService
                    .queryGetUserJobsByDateRange(userId, to, from);
        }
        else
            logger.info("Invalid User Id"+userId);
        return (List<JobStatistics>) userJobsByDateRange;
    }

    public boolean validateuserId(String userId){
        return Pattern.compile(REGEX_PATTERN)
                .matcher(userId)
                .matches();
    }
}
