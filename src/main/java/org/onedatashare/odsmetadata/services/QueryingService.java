package org.onedatashare.odsmetadata.services;

import org.springframework.beans.factory.annotation.Autowired;
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
    private final String QUERY_GETUSERJOBIDS ="select job_execution_id from batch_job_execution_params where string_val like= ?)";
    private final String QUERY_GETAllJOBSTATISTICSOFUSER ="select job_execution_id,start_time,end_time,status,last_updated from batch_job_execution where job_execution_id in (select job_execution_id from batch_job_execution_params where string_val = ?)";
    private final String QUERY_GETJOBSTAT ="select job_execution_id,start_time,end_time,status,last_updated from batch_job_execution where job_execution_id  = ?";
    private final String QUERY_GETUSERJOBSBYDATE ="select * from batch_job_execution where USERID = ? and start_time=?";
    private final String QUERY_GETUSERJOBSBYDATERANGE ="select jobIds from batch_job_execution where start_time <= and end_time >= ?";


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
    public List<String> queryUserJobIds(@NotNull String userId){
        return this.jdbcTemplate.queryForObject(QUERY_GETUSERJOBIDS,List.class,userId);
    }

    /**
     *
     * @param userId
     * @return
     */
    public Object queryGetAllJobStatisticsOfUser(@NotNull String userId) {
        return this.jdbcTemplate.queryForObject(QUERY_GETAllJOBSTATISTICSOFUSER,List.class,userId);
    }

    /**
     *
     * @param jobId
     * @return
     */

    public Object queryGetJobStat(@NotNull String jobId) {
        return this.jdbcTemplate.queryForObject(QUERY_GETJOBSTAT,List.class,jobId);
    }

    /**
     *
     * @param userId
     * @param date
     * @return
     */

    public Object queryGetUserJobsByDate(@NotNull String userId, Date date) {
        return this.jdbcTemplate.queryForObject(QUERY_GETUSERJOBSBYDATE,List.class,userId,date);
    }

    /**
     *
     * @param userId
     * @param to
     * @param from
     * @return
     */

    public Object queryGetUserJobsByDateRange(@NotNull String userId, Date to, Date from) {
        return this.jdbcTemplate.queryForObject(QUERY_GETUSERJOBSBYDATERANGE,List.class,userId,to,from);
    }
}
