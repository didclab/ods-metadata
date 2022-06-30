package org.onedatashare.odsmetadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Timestamp;

/**
 * This class represents the statistics regarding a job
 * Some examples are throughput, source, destination, time started, time completed more to come.
 */
@Data
@AllArgsConstructor
public class JobStatistic {
    int jobId;
    Timestamp startTime;
    Timestamp endTime;
    Status status;
    Timestamp lastUpdated;
    int readCount;
    int writeCount;
    String fileName;
    String strVal;


    public JobStatistic() {

    }
}
