package org.onedatashare.odsmetadata.services;

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
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Repository
public class QueryingService {
    @Autowired
    private JdbcTemplate jdbcTemplate;
    private final String QUERY_GETUSERJOBIDS ="select a.job_execution_id from batch_job_execution_params a " +
            "where a.string_val like ?";

    private final String QUERY_GETAllJOBSTATISTICSOFUSER ="select a.job_execution_id, a.start_time, a.end_time, a.status, " +
            "a.last_updated, b.read_count, b.write_count,c.type_cd,c.key_name, c.string_val, c.long_val from batch_job_execution a, " +
            "batch_step_execution b, batch_job_execution_params c where c.job_execution_id=a.job_execution_id and " +
            "b.job_execution_id=a.job_execution_id and a.job_execution_id in (select job_execution_id from " +
            "batch_job_execution_params where string_val like ?)";

    private final String QUERY_GETJOBSTAT =" select c.job_execution_id, a.start_time, a.end_time, a.status, " +
            "a.last_updated, b.read_count, b.write_count,c.type_cd,c.key_name, c.string_val, c.long_val from " +
            "batch_job_execution a, batch_step_execution b, batch_job_execution_params c where c.job_execution_id = ?  " +
            "and c.job_execution_id=a.job_execution_id and c.job_execution_id = b.job_execution_id and " +
            "a.job_execution_id = b.job_execution_id and b.step_execution_id in " +
            "(select min(step_execution_id) from batch_step_execution where job_execution_id = ? )";

    private final String QUERY_GETUSERJOBSBYDATE ="select a.job_execution_id,a.start_time,a.end_time, a.status" +
            ",a.last_updated,b.read_count,b.write_count,c.type_cd,c.key_name, c.string_val from batch_job_execution a" +
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
    private static final String TYP_CD = "type_cd";
    private static final String KEY_NAME = "key_name";
    private static final String STR_VAL = "string_val";
    private static final String LONG_VAL = "long_val";


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
                        ,rs.getInt(READ_COUNT),rs.getInt(WRITE_COUNT),rs.getString(TYP_CD),rs.getString(KEY_NAME),
                        rs.getString(STR_VAL),rs.getString(LONG_VAL)),userId);

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
                            ,rs.getInt(READ_COUNT),rs.getInt(WRITE_COUNT),rs.getString(TYP_CD),rs.getString(KEY_NAME),
                            rs.getString(STR_VAL),rs.getString(LONG_VAL)),
                    Integer.parseInt(jobId), Integer.parseInt(jobId));
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

    public List<JobStatistic> queryGetUserJobsByDate(@NotNull String userId, Instant date) {
        List<JobStatistic> list = new ArrayList<>();
        Date d = Date.from(date);
        HashMap<Integer, List<JobStatistic>> map = new HashMap<>();


        list = this.jdbcTemplate.query(QUERY_GETUSERJOBSBYDATE,(rs, rowNum) ->
                new JobStatistic(rs.getInt(JOB_EXECUTION_ID),
                        rs.getTimestamp(START_TIME),rs.getTimestamp(END_TIME),
                        Status.valueOf(rs.getString(STATUS).toLowerCase()),rs.getTimestamp(LAST_UPDATED)
                        ,rs.getInt(READ_COUNT),rs.getInt(WRITE_COUNT),rs.getString(TYP_CD),rs.getString(KEY_NAME),
                        rs.getString(STR_VAL),rs.getString(LONG_VAL)),userId,d);

        return list;
    }

    /**
     *
     * @param userId
     * @param to
     * @param from
     * @return
     */

    public List<Integer> queryGetUserJobsByDateRange(@NotNull String userId, Instant to, Instant from) {
        Date t = Date.from(to);
        Date f = Date.from(from);
        return this.jdbcTemplate.queryForList(QUERY_GETUSERJOBSBYDATERANGE,Integer.class,userId,t,f);
    }

    /**
     * Maps the list of JobStatistic to List<List<String>>
     *
     * @param jobStatisticList
     * @return
     */
    public Map<String, String> mapStringVal(List<JobStatistic> jobStatisticList) {

        Map<String, String> allStringVal = new HashMap<>();

        for(JobStatistic jobStatistic: jobStatisticList ){
            logger.info("type_cd: "+jobStatistic.getType_cd());
            if(jobStatistic.getType_cd().equals("LONG")){
                logger.info("step 169 if ");
                allStringVal.put(jobStatistic.getKeyVal(),jobStatistic.getLong_val());
            }
            else if(jobStatistic.getType_cd().equals("STRING")){
                logger.info("step 173 else-if ");
                allStringVal.put(jobStatistic.getKeyVal(),jobStatistic.getStrVal());
            }
        }
        logger.info("strval changes 166: "+allStringVal);
        return allStringVal;

    }

    /**
     * Converts list of Jobstatistic object to list of JobStatisticDto object
     *
     * @param anyJobStat
     * @return
     */

    public Set<JobStatisticDto> getJobStatisticDtos(List<JobStatistic> anyJobStat) {

        Set<JobStatisticDto> fileSet = new HashSet<>();
        List<JobStatisticDto> resultList = new ArrayList<>();

        for(int i=0;i< anyJobStat.size();i++){
            int currJobId = anyJobStat.get(i).getJobId();

            Set<Integer> readCountByJobId = anyJobStat.stream().
                    filter(job -> job.getJobId()==currJobId).map(f -> f.getReadCount()).collect(Collectors.toSet());
            Set<Integer> writeCountByJobId = anyJobStat.stream().
                    filter(job -> job.getJobId()==currJobId).map(f -> f.getWriteCount()).collect(Collectors.toSet());

            JobStatisticDto jobStatisticDto = new JobStatisticDto(anyJobStat.get(i).getJobId(),
                    anyJobStat.get(i).getStartTime(), anyJobStat.get(i).getEndTime(),
                    anyJobStat.get(i).getStatus(), anyJobStat.get(i).getLastUpdated(),
                    readCountByJobId, writeCountByJobId, mapStringVal(anyJobStat));

            fileSet.add(jobStatisticDto);
        }


        return fileSet;
    }

    private static boolean checkNull(Object obj){
        return  obj ==null || obj =="" ?true:false;
    }


}