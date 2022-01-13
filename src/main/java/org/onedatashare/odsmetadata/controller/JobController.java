package org.onedatashare.odsmetadata.controller;

import org.onedatashare.odsmetadata.model.JobStatistics;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * This controller allows a user to query jobs that they have submitted form CockroachDB
 */
@RestController
@RequestMapping("/api/v1/job")
public class JobController {

    /**
     * Returns all the jobs with the corresponding userId
     * This call should be done if you only want the JobIds
     * @param userId
     * @return List of jobIds
     */
    @GetMapping("/{userId}")
    public List<String> getUserJobIds(@PathVariable String userId){
        return new ArrayList<>();
    }

    /**
     * This is a bulk API call so if the user wants all information on all their jobs this is the right call
     * @param userId
     * @return A list of all JobStatistics involving a user
     */
    @GetMapping("/stats/{userId}")
    public List<JobStatistics> getAllJobStatisticsOfUser(@PathVariable String userId){
        return new ArrayList<>();
    }

    /**
     * Returns the meta data regarding any one job
     * @param jobId
     * @return
     */
    @GetMapping("/stats/{jobId}")
    public JobStatistics getJobStat(@PathVariable String jobId){
        return new JobStatistics();
    }

    /**
     * @param userId
     * @param date
     * @return
     */
    @GetMapping("/stats/{userId}/date")
    public List<JobStatistics> getUserJobsByDate(@PathVariable String userId, @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss") Date date){
        return new ArrayList<>();
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
        return new ArrayList<>();
    }
}
