package org.onedatashare.odsmetadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
public class JobStatisticDto {
    int jobId;
    Timestamp startTime;
    Timestamp endTime;
    Status status;
    Timestamp lastUpdated;
    String readCount;
    String writeCount;
    String fileName;
    JobParamDetails strVal;
}
