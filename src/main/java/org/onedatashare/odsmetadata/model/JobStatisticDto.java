package org.onedatashare.odsmetadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Timestamp;
import java.util.Set;

@Data
@AllArgsConstructor
public class JobStatisticDto {
    int jobId;
    Timestamp startTime;
    Timestamp endTime;
    Status status;
    Timestamp lastUpdated;
    Set<Integer> readCount;
    Set<Integer> writeCount;
    String fileName;
    JobParamDetails strVal;
}
