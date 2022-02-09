package org.onedatashare.odsmetadata.controller;

import com.google.common.base.Preconditions;
import org.onedatashare.odsmetadata.model.JobStatistics;
import org.onedatashare.odsmetadata.services.QueryingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.ws.rs.PathParam;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Pattern;

/**
 * This controller allows a user to query jobs that they have submitted form CockroachDB
 */
@RestController
@RequestMapping("/api/v1/meta")
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
    public List <String> getUserJobIds(@RequestParam(value="userId") String userId){
        ArrayList <String> userIdList = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        if(validateuserId(userId)) {
            logger.info(userId);
            userIdList = (ArrayList<@Valid String>) queryingService.queryUserJobIds(userId);
        }
        else
            logger.info("Invalid User Id"+userId);
        return userIdList;
    }

    /**
     * This is a bulk API call so if the user wants all information on all their jobs this is the right call
     * @param userId
     * @return A list of all JobStatistics involving a user
     */
    @GetMapping("/all_stats")
    public List<JobStatistics> getAllJobStatisticsOfUser(@RequestParam(value="userId") String userId){
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
    @GetMapping("/stat")
    public JobStatistics getJobStat(@RequestParam(value = "jobId") String jobId){
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
    @GetMapping("/stats/date")
    public List<JobStatistics> getUserJobsByDate(@RequestParam(value="userid") String userId, @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss") Date date){
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
        return userJobsBydate;
    }

    /**
     * @param userId
     * @param to
     * @param from
     * @return
     */
    @GetMapping("/stats/date/range")
    public List<JobStatistics> getUserJobsByDateRange(@RequestParam(value = "userId") String userId,
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
        logger.info(userId);
        return Pattern.compile(REGEX_PATTERN)
                .matcher(userId)
                .matches();
    }
}
