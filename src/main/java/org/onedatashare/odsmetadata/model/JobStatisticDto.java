package org.onedatashare.odsmetadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.List;
import java.util.Set;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobStatisticDto {
    long jobId;
    Timestamp startTime;
    Timestamp endTime;
    Status status;
    Timestamp lastUpdated;
    Set<Integer> readCount;
    Set<Integer> writeCount;
    String fileName;
    JobParamDetails strVal;
    List<InfluxData> jobMeasurements;
}
