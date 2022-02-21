package org.onedatashare.odsmetadata.services;

import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.onedatashare.odsmetadata.model.JobStatistics;
import org.onedatashare.odsmetadata.model.Status;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.sql.ResultSet;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * Unit Test class for QueryingService
 *
 */
@RunWith(MockitoJUnitRunner.class)
public class QueryingServiceTest {
   @InjectMocks
   private QueryingService queryingService;

    @Before
    public void init(){
        MockitoAnnotations.openMocks(this);
    }

    /**
     * Test for queryUserJobIdsTest
     */
    @Test
    public void queryUserJobIdsTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);

        List<Integer> jobIds = new ArrayList<>();
        jobIds.add(172);

        Mockito.when(queryingService.queryUserJobIds("abcd@test.com")).thenReturn(jobIds);
        Assert.assertEquals(jobIds, queryingService.queryUserJobIds("abcd@test.com"));
    }

    /**
     * Test for queryUserJobIdsTest when userId is null
     */
    @Test(expected=NullPointerException.class)
    public void queryUserJobIdsNullTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);

        Mockito.when(queryingService.queryUserJobIds(null)).thenThrow(NullPointerException.class);
        Assert.assertTrue(queryingService.queryUserJobIds(null) instanceof NullPointerException);
    }

    /**
     * Test for queryGetAllJobStatisticsOfUserTest
     */
    @Test
    public void queryGetAllJobStatisticsOfUserTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);
        List<JobStatistics> listStats = new ArrayList<>();

        String date_string_start = "26-09-2021";
        String date_string_end = "27-09-2021";
        Date start_date;
        Date end_date;
        int read_count=2;
        int write_count=2;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        try {
            start_date = formatter.parse(date_string_start);
            end_date = formatter.parse(date_string_end);
            JobStatistics stats = new JobStatistics(172, (Timestamp) start_date, (Timestamp) end_date,
                    Status.completed, (Timestamp) end_date,read_count,write_count);
            listStats.add(stats);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        String QUERY_GETAllJOBSTATISTICSOFUSER ="select job_execution_id,start_time,end_time,status,last_updated from " +
                "batch_job_execution where job_execution_id in (select job_execution_id from batch_job_execution_params " +
                "where string_val = ?)";

        Mockito.when(jdbcTemplate.query(QUERY_GETAllJOBSTATISTICSOFUSER,(ResultSet rs, int rowNum) -> {

            JobStatistics s = new JobStatistics(172, (Timestamp) new Date(), (Timestamp) new Date(),
                    Status.completed,(Timestamp)new Date(),read_count,write_count);
            List<JobStatistics> sList = new ArrayList<>();
            sList.add (s);return sList;

        },"jacobgol@buffalo.edu")).thenReturn(Collections.singletonList(listStats));
        Mockito.when(queryingService.queryGetAllJobStatisticsOfUser("jacobgol@buffalo.edu")).thenReturn(listStats);
        Assert.assertEquals(listStats, queryingService.queryGetAllJobStatisticsOfUser("jacobgol@buffalo.edu"));
    }

    /**
     * Test for queryGetJobStatTest
     */
    @Test
    @SneakyThrows
    public void queryGetJobStatTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);

        String date_string_start = "26-09-2021";
        String date_string_end = "27-09-2021";
        Date start_date;
        Date end_date;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        int read_count=2;
        int write_count=2;

        start_date = formatter.parse(date_string_start);
        end_date = formatter.parse(date_string_end);
        JobStatistics stats = new JobStatistics(172, (Timestamp) start_date, (Timestamp) end_date,
                Status.completed,(Timestamp)end_date,read_count,write_count);

        Mockito.when(queryingService.queryGetJobStat("123")).thenReturn(stats);
        Assert.assertEquals(stats, queryingService.queryGetJobStat("123"));
    }

    /**
     * Test for queryGetUserJobsByDateTest
     */
    @SneakyThrows
    @Test
    public void queryGetUserJobsByDateTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);
        List<JobStatistics> statsList = new ArrayList<>();

        String date_string_start = "26-09-2021";
        String date_string_end = "27-09-2021";
        Date start_date;
        Date end_date;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
        int read_count=2;
        int write_count=2;

        start_date = formatter.parse(date_string_start);
        end_date = formatter.parse(date_string_end);
        JobStatistics stats = new JobStatistics(172, (Timestamp) start_date, (Timestamp) end_date,
                Status.completed,(Timestamp)end_date,read_count,write_count);
        statsList.add(stats);

        final String QUERY_GETUSERJOBSBYDATE ="select job_execution_id,start_time,end_time,status,last_updated " +
                "from batch_job_execution where job_execution_id in (select job_execution_id from " +
                "batch_job_execution_params " +
                "where string_val like ?) and start_time=?";

        Mockito.when(jdbcTemplate.query(QUERY_GETUSERJOBSBYDATE,(ResultSet rs, int rowNum) -> {

            JobStatistics s = new JobStatistics(172, (Timestamp) start_date, (Timestamp) end_date,
                    Status.completed, (Timestamp) new Date(),read_count,write_count);
            List<JobStatistics> sList = new ArrayList<>();
            sList.add (s);return sList;

        },"jacobgol@buffalo.edu")).thenReturn(Collections.singletonList(statsList));

        Mockito.when(queryingService.queryGetUserJobsByDate("jacobgol@buffalo.edu",start_date))
                .thenReturn(statsList);

        Assert.assertEquals(statsList, queryingService.queryGetUserJobsByDate("jacobgol@buffalo.edu",start_date));
    }

    /**
     * Test to check successful behavior of queryGetUserJobsByDateRange
     *
     */
    @SneakyThrows
    @Test
    public void queryGetUserJobsByDateRangeTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);

        String date_string_start = "26-09-2021";
        String date_string_end = "27-09-2021";
        Date start_date;
        Date end_date;
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

        start_date = formatter.parse(date_string_start);
        end_date = formatter.parse(date_string_end);

        List<Integer> jobIds = new ArrayList<>();
        jobIds.add(172);

        final String QUERY_GETUSERJOBSBYDATERANGE = "select job_execution_id from batch_job_execution " +
                "where job_execution_id " +
                "in (select job_execution_id from batch_job_execution_params where string_val like ?) " +
                "and start_time <=? " +
                "and end_time >=?";


        Mockito.when(jdbcTemplate.queryForList(QUERY_GETUSERJOBSBYDATERANGE, Integer.class, start_date, end_date ))
                .thenReturn(jobIds);
        Mockito.when(queryingService.queryGetUserJobsByDateRange("abcd@test.com", start_date, end_date))
                .thenReturn(jobIds);
        Assert.assertEquals(jobIds, queryingService.queryGetUserJobsByDateRange("abcd@test.com", start_date,
                end_date));

    }
}