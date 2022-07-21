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
    int jobId; //this should be a long
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
    // here we want a list of of the values from the batch_step_exeuction table
    //we also want a map that will hold the jobParams
    //remove read and write count as they r just wrong
    //also remove the keyVal,strVal and long val as those r somehow the JobParams.
}