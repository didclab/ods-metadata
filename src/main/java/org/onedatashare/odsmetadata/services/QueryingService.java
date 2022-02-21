package org.onedatashare.odsmetadata.services;

import org.onedatashare.odsmetadata.model.JobStatistics;
import org.onedatashare.odsmetadata.model.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import javax.validation.constraints.NotNull;
import java.util.Date;
import java.util.List;

@Repository
public class QueryingService {
    @Autowired
    private JdbcTemplate jdbcTemplate;
    private final String QUERY_GETUSERJOBIDS ="select a.job_execution_id from batch_job_execution_params a " +
            "where a.string_val like ?";

    private final String QUERY_GETAllJOBSTATISTICSOFUSER ="select a.job_execution_id, a.start_time, a.end_time, a.status" +
            ",a.last_updated, b.read_count, b.write_count from batch_job_execution a, batch_step_execution b " +
            "where a.job_execution_id in (select job_execution_id from batch_job_execution_params where string_val like ?)";

    private final String QUERY_GETJOBSTAT =" select a.job_execution_id, a.start_time, a.end_time, a.status, " +
            "a.last_updated, b.read_count, b.write_count from batch_job_execution a,batch_step_execution b " +
            "where a.job_execution_id=b.job_execution_id and a.job_execution_id= ?";

    private final String QUERY_GETUSERJOBSBYDATE ="select a.job_execution_id,a.start_time,a.end_time, a.status" +
            ",a.last_updated,b.read_count,b.write_count from batch_job_execution a,batch_step_execution b  " +
            "where a.job_execution_id in (select job_execution_id from batch_job_execution_params " +
            "where string_val like ?) and a.start_time=?";

    private final String QUERY_GETUSERJOBSBYDATERANGE ="select a.job_execution_id from batch_job_execution a " +
            "where a.job_execution_id in (select job_execution_id from batch_job_execution_params where " +
            "string_val like ?) and a.start_time <=? and a.end_time >=?";

    public static final String START_TIME = "start_time";
    public static final String END_TIME = "end_time";
    public static final String JOB_EXECUTION_ID = "job_execution_id";
    public static final String STATUS = "status";
    public static final String LAST_UPDATED = "last_updated";
    public static final String READ_COUNT = "read_count";
    public static final String WRITE_COUNT = "write_count";


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
    public List<JobStatistics> queryGetAllJobStatisticsOfUser(@NotNull String userId){

        return this.jdbcTemplate.query(QUERY_GETAllJOBSTATISTICSOFUSER,
                (rs, rowNum) -> new JobStatistics(rs.getInt(JOB_EXECUTION_ID),
                        rs.getTimestamp(START_TIME),rs.getTimestamp(END_TIME),
                        Status.valueOf(rs.getString(STATUS).toLowerCase()),rs.getTimestamp(LAST_UPDATED)
                        ,rs.getInt(READ_COUNT),rs.getInt(WRITE_COUNT)),userId);

    }

    /**
     *
     * @param jobId
     * @return
     */

    public JobStatistics queryGetJobStat(@NotNull String jobId) {
        try {
            return this.jdbcTemplate.queryForObject(QUERY_GETJOBSTAT,
                    (rs, rowNum) -> new JobStatistics(rs.getInt(JOB_EXECUTION_ID),
                            rs.getTimestamp(START_TIME),rs.getTimestamp(END_TIME),
                            Status.valueOf(rs.getString(STATUS).toLowerCase()),rs.getTimestamp(LAST_UPDATED)
                            ,rs.getInt(READ_COUNT),rs.getInt(WRITE_COUNT)),
                    Integer.parseInt(jobId));
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

    public List<JobStatistics> queryGetUserJobsByDate(@NotNull String userId, Date date) {
        return this.jdbcTemplate.query(QUERY_GETUSERJOBSBYDATE,(rs, rowNum) ->
                new JobStatistics(rs.getInt(JOB_EXECUTION_ID),
                rs.getTimestamp(START_TIME),rs.getTimestamp(END_TIME),
                Status.valueOf(rs.getString(STATUS).toLowerCase()),rs.getTimestamp(LAST_UPDATED)
                ,rs.getInt(READ_COUNT),rs.getInt(WRITE_COUNT)),userId,date);
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
}
