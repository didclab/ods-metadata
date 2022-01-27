package org.onedatashare.odsmetadata.services;

import lombok.SneakyThrows;
import org.junit.Assert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.onedatashare.odsmetadata.controller.JobController;
import java.util.Collections;
import java.text.SimpleDateFormat;

import static org.mockito.Mockito.when;
@RunWith(MockitoJUnitRunner.class)
class QueryingServiceTest {
    @InjectMocks
    JobController jobControllerObject = new JobController();

    @Mock
    QueryingService queryingService;

    @BeforeEach
    void setUp() {
    }

    @Test
    void queryUserJobIds() {
        when(queryingService.queryUserJobIds("jacobgol@buffalo.edu")).thenReturn(Collections.singletonList("Found"));
        Assert.assertEquals(String.valueOf(jobControllerObject.getUserJobIds("jacobgol@buffalo.edu"))
                ,"Found", "Not Found");
    }

    @Test
    void queryGetAllJobStatisticsOfUser() {
        when(queryingService.queryGetAllJobStatisticsOfUser("jacobgol@buffalo.edu"))
                .thenReturn(Collections.singletonList("Found"));
        Assert.assertEquals(String.valueOf(jobControllerObject.getAllJobStatisticsOfUser("jacobgol@buffalo.edu"))
                ,"Found", "Not Found");
    }

    @Test
    void queryGetJobStat() {
        when(queryingService.queryGetJobStat("123")).thenReturn(Collections.singletonList("Found"));
        Assert.assertEquals(String.valueOf(jobControllerObject.getJobStat("123")),"Found"
                , "Not Found");
    }

    @SneakyThrows
    @Test
    void queryGetUserJobsByDate() {
       SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        when(queryingService.queryGetUserJobsByDate("123", date.parse("1970-01-01 19:00:00")))
                .thenReturn("Found");
        Assert.assertEquals(String.valueOf(jobControllerObject
                .getUserJobsByDate("123", date.parse("1970-01-01 19:00:00")))
                ,"Found", "Not Found");
    }

    @SneakyThrows
    @Test
    void queryGetUserJobsByDateRange() {
        SimpleDateFormat todate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat enddate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        when(queryingService.queryGetUserJobsByDateRange("123", todate.parse("1970-01-01 19:00:00"),
                enddate.parse("1970-01-01 19:00:00"))).thenReturn("Found");
        Assert.assertEquals(String.valueOf(jobControllerObject.getUserJobsByDateRange("123",
                todate.parse("1970-01-01 19:00:00"),
                enddate.parse("1970-01-01 19:00:00"))),"Found", "Not Found");
    }
}