package org.onedatashare.odsmetadata.controller;

import com.google.common.base.Preconditions;
import org.onedatashare.odsmetadata.model.JobParamDetails;
import org.onedatashare.odsmetadata.model.JobStatistic;
import org.onedatashare.odsmetadata.model.JobStatisticDto;
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
import java.util.stream.Collectors;

/**
 * This controller allows a user to query jobs that they have submitted form CockroachDB
 */
@RestController
@RequestMapping(value="/api/v1/meta", produces = MediaType.APPLICATION_JSON_VALUE)
public class JobController {

    private static final Logger LOG = LoggerFactory.getLogger(JobController.class);
    @Autowired
    QueryingService queryingService;
    private static final Logger logger = LoggerFactory.getLogger(JobController.class);
    private static final String REGEX_PATTERN = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
            + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$"; //this is used to validate that the userId is an email
    private static final String REGEX = "\\d+";


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
    public List<JobStatisticDto> getAllJobStatisticsOfUser(@RequestParam(value="userId") String userId){
        List <JobStatistic> allJobStatsOfUser = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if(validateuserId(userId)) {
            allJobStatsOfUser =  queryingService.queryGetAllJobStatisticsOfUser(userId);
        }
        List<String> strList = queryingService.mapStringVal(allJobStatsOfUser)
                .stream()
                .flatMap( l ->  l.stream()).collect(Collectors.toList());

        JobParamDetails jobParamDetails = queryingService.mapStrVal(strList);

        JobStatisticDto jobStatisticDto = new JobStatisticDto(allJobStatsOfUser.get(0).getJobId(),
                allJobStatsOfUser.get(0).getStartTime(), allJobStatsOfUser.get(0).getEndTime(),
                allJobStatsOfUser.get(0).getStatus(), allJobStatsOfUser.get(0).getLastUpdated(),
                allJobStatsOfUser.get(0).getReadCount(), allJobStatsOfUser.get(0).getWriteCount(),
                allJobStatsOfUser.get(0).getFileName(), jobParamDetails);

        String res= String.valueOf(strList);
        List<JobStatisticDto> list = new ArrayList<>();
        allJobStatsOfUser.get(0).setStrVal(res);
        return Arrays.asList(jobStatisticDto);
    }




    /**
     * Returns the meta data regarding any one job
     * @param jobId
     * @return
     */
    @GetMapping("/stat")
    public List<JobStatisticDto> getJobStat(@RequestParam(value = "jobId") String jobId){
        List<JobStatistic> anyJobStat = Collections.emptyList();
        logger.info(jobId);
        if(jobId.matches(REGEX)) {
            anyJobStat = queryingService.queryGetJobStat(jobId);
        }
        List<String> strList = queryingService.mapStringVal(anyJobStat)
                .stream()
                .flatMap( l ->  l.stream()).collect(Collectors.toList());

        JobParamDetails jobParamDetails = queryingService.mapStrVal(strList);

        JobStatisticDto jobStatisticDto = new JobStatisticDto(anyJobStat.get(0).getJobId(),
                anyJobStat.get(0).getStartTime(), anyJobStat.get(0).getEndTime(),
                anyJobStat.get(0).getStatus(), anyJobStat.get(0).getLastUpdated(),
                anyJobStat.get(0).getReadCount(), anyJobStat.get(0).getWriteCount(),
                anyJobStat.get(0).getFileName(), jobParamDetails);

        String res= String.valueOf(strList);
        List<JobStatisticDto> list = new ArrayList<>();
        anyJobStat.get(0).setStrVal(res);
        return Arrays.asList(jobStatisticDto);

    }

    /**
     * @param userId
     * @param date
     * @return
     */
    @GetMapping("/stats/date")
    public List<JobStatisticDto> getUserJobsByDate(@RequestParam(value="userId") String userId, @RequestParam(value="date")
                                                 @DateTimeFormat(pattern="yyyy-MM-dd'T'HH:mm:ss.SSS") Date date){
        List<JobStatistic> userJobsBydate = new ArrayList<>();
        Preconditions.checkNotNull(userId);
        logger.info(userId);
        if(validateuserId(userId)) {
            userJobsBydate = queryingService.queryGetUserJobsByDate(userId, date);
        }
        List<String> strList = queryingService.mapStringVal(userJobsBydate)
                .stream()
                .flatMap( l ->  l.stream()).collect(Collectors.toList());

        JobParamDetails jobParamDetails = queryingService.mapStrVal(strList);

        JobStatisticDto jobStatisticDto = new JobStatisticDto(userJobsBydate.get(0).getJobId(),
                userJobsBydate.get(0).getStartTime(), userJobsBydate.get(0).getEndTime(),
                userJobsBydate.get(0).getStatus(), userJobsBydate.get(0).getLastUpdated(),
                userJobsBydate.get(0).getReadCount(), userJobsBydate.get(0).getWriteCount(),
                userJobsBydate.get(0).getFileName(), jobParamDetails);

        String res= String.valueOf(strList);
        List<JobStatisticDto> list = new ArrayList<>();
        userJobsBydate.get(0).setStrVal(res);
        return Arrays.asList(jobStatisticDto);
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
