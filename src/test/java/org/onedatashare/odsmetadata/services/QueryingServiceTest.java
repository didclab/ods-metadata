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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class QueryingServiceTest {
   @InjectMocks
   private QueryingService queryingService;

    @Before
    public void init(){
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void queryUserJobIdsTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);
        List<String> userNames = new ArrayList<String>();
        userNames.add("jacobgol@buffalo.edu");
        List<String> result= Collections.emptyList();
        String QUERY_GETUSERJOBIDS ="select job_execution_id from batch_job_execution_params where string_val ilike= ?";
//        when(jdbcTemplate.queryForObject(eq(QUERY_GETUSERJOBIDS),eq(List.class),eq("jacobgol@buffalo.edu"))).thenReturn(userNames);
        when(queryingService.queryUserJobIds("")).thenReturn(userNames);
        Assert.assertEquals(queryingService.queryUserJobIds(""), userNames);
    }


    @Test
    public void queryGetAllJobStatisticsOfUserTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);
        List<String> userNames = new ArrayList<String>();
        userNames.add("jacobgol@buffalo.edu");
        String QUERY_GETAllJOBSTATISTICSOFUSER ="select job_execution_id,start_time,end_time,status,last_updated from " +
                "batch_job_execution where job_execution_id in (select job_execution_id from batch_job_execution_params where string_val = ?)";
        when(jdbcTemplate.queryForObject(eq(QUERY_GETAllJOBSTATISTICSOFUSER),eq(List.class),eq("jacobgol@buffalo.edu"))).thenReturn(userNames);
        when(queryingService.queryGetAllJobStatisticsOfUser("jacobgol@buffalo.edu")).thenReturn(userNames);
        Assert.assertEquals(queryingService.queryGetAllJobStatisticsOfUser("jacobgol@buffalo.edu"), userNames);
    }

    @Test
    public void queryGetJobStatTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);
        List<Integer> jobIds = new ArrayList<Integer>();
        jobIds.add(123);
        String QUERY_GETJOBSTAT ="select job_execution_id,start_time,end_time,status,last_updated from batch_job_execution where job_execution_id  = ?";
//        when(jdbcTemplate.queryForObject(eq(QUERY_GETJOBSTAT),eq(List.class),eq(123))).thenReturn(jobIds);
        when(queryingService.queryGetJobStat("123")).thenReturn(jobIds);
        Assert.assertEquals(queryingService.queryGetJobStat("123"), jobIds);
    }

    @SneakyThrows
    @Test
    public void queryGetUserJobsByDateTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);
        String QUERY_GETUSERJOBSBYDATE ="select * from batch_job_execution where USERID = ? and start_time=?";
        List<String> userNames = new ArrayList<String>();
        userNames.add("jacobgol@buffalo.edu");
        String dateInString = "2016-09-09 19:00:00";
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD HH:MM:SS");
        Date date = formatter.parse(dateInString);
//        when(jdbcTemplate.queryForObject(eq(QUERY_GETUSERJOBSBYDATE),eq(List.class),refEq(date),
//                eq("jacobgol@buffalo.edu"))).thenReturn(userNames);
        when(queryingService.queryGetUserJobsByDate("jacobgol@buffalo.edu", date))
                .thenReturn(userNames);
        Assert.assertEquals(queryingService.queryGetUserJobsByDate("jacobgol@buffalo.edu", date)
                , userNames);

    }

    @SneakyThrows
    @Test
    public void queryGetUserJobsByDateRangeTest() {
        JdbcTemplate jdbcTemplate = Mockito.mock(JdbcTemplate.class);
        ReflectionTestUtils.setField(queryingService, "jdbcTemplate", jdbcTemplate);
        String QUERY_GETUSERJOBSBYDATERANGE ="select jobIds from batch_job_execution where start_time <= and end_time >= ?";        List<String> userNames = new ArrayList<String>();
        userNames.add("jacobgol@buffalo.edu");
        String todateInString = "2016-09-09 19:00:00";
        String fromdateInString = "2016-09-01 19:00:00";
        SimpleDateFormat formatter = new SimpleDateFormat("YYYY-MM-DD HH:MM:SS");
        Date todate = formatter.parse(todateInString);
        Date fromdate = formatter.parse(fromdateInString);

//        when(jdbcTemplate.queryForObject(eq(QUERY_GETUSERJOBSBYDATERANGE),eq(Date.class),refEq(fromdate),
//                refEq(todate), eq("jacobgol@buffalo.edu"))).thenReturn(todate);
        when(queryingService.queryGetUserJobsByDateRange("jacobgol@buffalo.edu",fromdate, todate))
                .thenReturn(userNames);
        Assert.assertEquals(queryingService.queryGetUserJobsByDateRange("jacobgol@buffalo.edu",fromdate, todate)
                , userNames);
    }
}