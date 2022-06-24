package org.onedatashare.odsmetadata.services;

import org.onedatashare.odsmetadata.model.JobParamDetails;
import org.onedatashare.odsmetadata.model.JobStatistic;
import org.onedatashare.odsmetadata.model.JobStatisticDto;
import org.onedatashare.odsmetadata.model.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

@Repository
public class QueryingService {
    @Autowired
    private JdbcTemplate jdbcTemplate;
    private final String QUERY_GETUSERJOBIDS ="select a.job_execution_id from batch_job_execution_params a " +
            "where a.string_val like ?";

    private final String QUERY_GETAllJOBSTATISTICSOFUSER ="select a.job_execution_id, a.start_time, a.end_time, a.status, " +
            "a.last_updated, b.read_count, b.write_count, c.string_val, b.step_name from batch_job_execution a, " +
            "batch_step_execution b, batch_job_execution_params c where c.job_execution_id=a.job_execution_id and " +
            "b.job_execution_id=a.job_execution_id and a.job_execution_id in (select job_execution_id from " +
            "batch_job_execution_params where string_val like ?)";

    private final String QUERY_GETJOBSTAT =" select a.job_execution_id, a.start_time, a.end_time, a.status, " +
            "a.last_updated, b.read_count, b.write_count, c.string_val, b.step_name from batch_job_execution a, " +
            "batch_step_execution b, batch_job_execution_params c where c.job_execution_id in " +
            "(select job_execution_id from batch_job_execution where job_execution_id=?) and b.job_execution_id in " +
            "(select job_execution_id from batch_job_execution where job_execution_id=?) and a.job_execution_id= ?";

    private final String QUERY_GETUSERJOBSBYDATE ="select a.job_execution_id,a.start_time,a.end_time, a.status" +
            ",a.last_updated,b.read_count,b.write_count,b.step_name,c.string_val from batch_job_execution a" +
            ",batch_step_execution b,batch_job_execution_params c  " +
            "where c.job_execution_id=a.job_execution_id and a.job_execution_id=b.job_execution_id and " +
            "a.job_execution_id in (select job_execution_id from batch_job_execution_params " +
            "where string_val like ?) and a.start_time=?";

    private final String QUERY_GETUSERJOBSBYDATERANGE ="select a.job_execution_id from batch_job_execution a " +
            "where a.job_execution_id in (select job_execution_id from batch_job_execution_params where " +
            "string_val like ?) and a.start_time <=? and a.end_time >=?";

    private static final String START_TIME = "start_time";
    private static final String END_TIME = "end_time";
    private static final String JOB_EXECUTION_ID = "job_execution_id";
    private static final String STATUS = "status";
    private static final String LAST_UPDATED = "last_updated";
    private static final String READ_COUNT = "read_count";
    private static final String WRITE_COUNT = "write_count";
    private static final String FILE_NAME = "step_name";
    private static final String STR_VAL = "string_val";


    private static final Logger logger = LoggerFactory.getLogger(QueryingService.class);

    /**
     * @param dataSource
     */
    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    /**
     *
     * @param userId
     * @return
     */
    public List<Integer> queryUserJobIds(@NotNull String userId){
        return this.jdbcTemplate.queryForList(QUERY_GETUSERJOBIDS,Integer.class,userId);
    }

    /**
     *
     * @param userId
     * @return
     */
    public List<JobStatistic> queryGetAllJobStatisticsOfUser(@NotNull String userId){

        return this.jdbcTemplate.query(QUERY_GETAllJOBSTATISTICSOFUSER,
                (rs, rowNum) -> new JobStatistic(rs.getInt(JOB_EXECUTION_ID),
                        rs.getTimestamp(START_TIME),rs.getTimestamp(END_TIME),
                        Status.valueOf(rs.getString(STATUS).toLowerCase()),rs.getTimestamp(LAST_UPDATED)
                        ,rs.getInt(READ_COUNT),rs.getInt(WRITE_COUNT),rs.getString(FILE_NAME),rs.getString(STR_VAL)),userId);

    }

    /**
     *
     * @param jobId
     * @return
     */
    public List<JobStatistic> queryGetJobStat(@NotNull String jobId) {
        try {
            return this.jdbcTemplate.query(QUERY_GETJOBSTAT,
                    (rs, rowNum) -> new JobStatistic(rs.getInt(JOB_EXECUTION_ID),
                            rs.getTimestamp(START_TIME),rs.getTimestamp(END_TIME),
                            Status.valueOf(rs.getString(STATUS).toLowerCase()),rs.getTimestamp(LAST_UPDATED)
                            ,rs.getInt(READ_COUNT),rs.getInt(WRITE_COUNT),rs.getString(FILE_NAME),rs.getString(STR_VAL)),
                    Integer.parseInt(jobId),Integer.parseInt(jobId),Integer.parseInt(jobId));
        }
        catch(IncorrectResultSizeDataAccessException ex) {
            if(logger.isDebugEnabled()) {
                logger.debug(String.format("Result set is either null or >1: %s ",ex.getStackTrace().toString()));
            }
            return null;
        }

    }

    /**
     *
     * @param userId
     * @param date
     * @return
     */

    public List<JobStatistic> queryGetUserJobsByDate(@NotNull String userId, Date date) {
        List<JobStatistic> list = new ArrayList<>();

        HashMap<Integer, List<JobStatistic>> map = new HashMap<>();


        list = this.jdbcTemplate.query(QUERY_GETUSERJOBSBYDATE,(rs, rowNum) ->
                new JobStatistic(rs.getInt(JOB_EXECUTION_ID),
                        rs.getTimestamp(START_TIME),rs.getTimestamp(END_TIME),
                        Status.valueOf(rs.getString(STATUS).toLowerCase()),rs.getTimestamp(LAST_UPDATED)
                        ,rs.getInt(READ_COUNT),rs.getInt(WRITE_COUNT),rs.getString(FILE_NAME),rs.getString(STR_VAL)),userId,date);

        return list;
    }

    /**
     *
     * @param userId
     * @param to
     * @param from
     * @return
     */

    public List<Integer> queryGetUserJobsByDateRange(@NotNull String userId, Date to, Date from) {
        return this.jdbcTemplate.queryForList(QUERY_GETUSERJOBSBYDATERANGE,Integer.class,userId,from,to);
    }

    /**
     * Maps the list of JobStatistic to List<List<String>>
     *
     * @param jobStatisticList
     * @return
     */
    public List<List<String>> mapStringVal(List<JobStatistic> jobStatisticList) {
        Map<Integer, List<String>> allStringVal = jobStatisticList
                .stream()
                .collect(Collectors.groupingBy(a -> a.getJobId(),Collectors.mapping(m -> m.getStrVal(), toList())));

        List<String> str= new ArrayList<>();

        for (JobStatistic i : jobStatisticList) {
            String temp = i.getStrVal();
            str.add(temp);
        }

        JobParamDetails jobParamDetails = mapStrVal(str);
        return allStringVal.entrySet().stream().map(v -> v.getValue()).collect(Collectors.toList());

    }

    /**
     * Converts a  list of string strlist to JobParamDetails
     *
     * @param strList
     * @return
     */

    public JobParamDetails mapStrVal(List<String> strList) {
        if(strList.isEmpty()){
            return null;
        }
        JobParamDetails jobParamDetails = new JobParamDetails();
        for (int i = 0; i < strList.size(); i++) {
            jobParamDetails.setTime(strList.get(0));
            jobParamDetails.setOwnerId(strList.get(1));
            jobParamDetails.setPriority(strList.get(2));
            jobParamDetails.setChunkSize(strList.get(3));
            jobParamDetails.setSourcePath(strList.get(4));
            jobParamDetails.setDestPath(strList.get(5));
            jobParamDetails.setSourceCreds(strList.get(6));
            jobParamDetails.setDestCreds(strList.get(7));
            jobParamDetails.setCompress(strList.get(8));
            jobParamDetails.setConcurrency(strList.get(9));
            jobParamDetails.setPipelining(strList.get(10));
            jobParamDetails.setParallelism(strList.get(11));
            jobParamDetails.setRetry(strList.get(12));
            jobParamDetails.setFileDetails(strList.get(13));

        }
        if (checkNull(jobParamDetails.getTime())){
            jobParamDetails.setTime("00:00:00");
        }
        if(checkNull(jobParamDetails.getOwnerId())){
            jobParamDetails.setOwnerId("Onedatashare");
        }
        if(checkNull(jobParamDetails.getPriority())){
            jobParamDetails.setPriority("1");
        }
        if(checkNull(jobParamDetails.getChunkSize().isEmpty())){
            jobParamDetails.setChunkSize("1");
        }
        if(!checkNull(jobParamDetails.getConcurrency()) && jobParamDetails.getConcurrency().contains(",")
                || checkNull(jobParamDetails.getConcurrency())){
            jobParamDetails.setConcurrency("0");
        }
        if(checkNull(jobParamDetails.getSourcePath())){
            jobParamDetails.setSourcePath("/Onedatashare");
        }
        if(checkNull(jobParamDetails.getDestPath())){
            jobParamDetails.setDestPath("/Owner");
        }
        if(checkNull(jobParamDetails.getSourceCreds())){
            jobParamDetails.setSourceCreds("Source");
        }
        if(checkNull(jobParamDetails.getDestCreds())){
            jobParamDetails.setDestCreds("Destination");
        }
        if(checkNull(jobParamDetails.getCompress()) || !checkNull(jobParamDetails.getCompress()) &&
                jobParamDetails.getCompress().contains(",")){
            jobParamDetails.setCompress("0");
        }
        if(checkNull(jobParamDetails.getPipelining()) || !checkNull(jobParamDetails.getPipelining()) &&
                jobParamDetails.getPipelining().contains(",")){
            jobParamDetails.setPipelining("0");
        }
        if(checkNull(jobParamDetails.getParallelism()) && !checkNull(jobParamDetails.getParallelism()) &&
                jobParamDetails.getParallelism().contains(",")){
            jobParamDetails.setParallelism("0");
        }
        if(checkNull(jobParamDetails.getRetry()) || !checkNull(jobParamDetails.getRetry())  &&
                jobParamDetails.getRetry().contains(",")){
            jobParamDetails.setRetry("0");
        }
        if(checkNull(jobParamDetails.getFileDetails()) || !checkNull(jobParamDetails.getFileDetails()) &&
                jobParamDetails.getFileDetails().contains(" ,")){
            jobParamDetails.setFileDetails("Ondateshare File");
        }


        return  jobParamDetails;
    }

    /**
     * Converts list of Jobstatistic object to list of JobStatisticDto object
     *
     * @param anyJobStat
     * @return
     */

    public Set<JobStatisticDto> getJobStatisticDtos(List<JobStatistic> anyJobStat) {
        List<String> strList = mapStringVal(anyJobStat)
                .stream()
                .flatMap( l ->  l.stream()).collect(Collectors.toList());

        JobParamDetails jobParamDetails = mapStrVal(strList);
        if(jobParamDetails ==null){
            return Collections.emptySet();
        }
        Set<JobStatisticDto> fileSet = new HashSet<>();

        List<JobStatisticDto> resultList = new ArrayList<>();


        for(int i=0;i< anyJobStat.size();i++){
            int currJobId = anyJobStat.get(i).getJobId();
            Set<String> fileNamesByJobId = anyJobStat.stream().
                    filter(job -> job.getJobId()==currJobId).map(f -> f.getFileName()).collect(Collectors.toSet());
            Set<Integer> readCountByJobId = anyJobStat.stream().
                    filter(job -> job.getJobId()==currJobId).map(f -> f.getReadCount()).collect(Collectors.toSet());
            Set<Integer> writeCountByJobId = anyJobStat.stream().
                    filter(job -> job.getJobId()==currJobId).map(f -> f.getWriteCount()).collect(Collectors.toSet());

            JobStatisticDto jobStatisticDto = new JobStatisticDto(anyJobStat.get(i).getJobId(),
                    anyJobStat.get(i).getStartTime(), anyJobStat.get(i).getEndTime(),
                    anyJobStat.get(i).getStatus(), anyJobStat.get(i).getLastUpdated(),
                    readCountByJobId.stream().map(String::valueOf).collect(Collectors.joining(",")),
                    writeCountByJobId.stream().map(String::valueOf).collect(Collectors.joining(",")),
                    String.join(", ", fileNamesByJobId), jobParamDetails);

            fileSet.add(jobStatisticDto);
        }

        String res= String.valueOf(strList);
        anyJobStat.get(0).setStrVal(res);
        return fileSet;
    }

    private static boolean checkNull(Object obj){
        return  obj ==null || obj =="" ?true:false;
    }


}