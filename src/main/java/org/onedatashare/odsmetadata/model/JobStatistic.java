package org.onedatashare.odsmetadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * This class represents the statistics regarding a job
 * Some examples are throughput, source, destination, time started, time completed more to come.
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobStatistic {
    int jobId;
    Timestamp startTime;
    Timestamp endTime;
    Status status;
    Timestamp lastUpdated;
    int readCount;
    int writeCount;
    String type_cd;
    String keyVal;
    String strVal;
    String long_val;
}