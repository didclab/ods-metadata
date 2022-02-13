package org.onedatashare.odsmetadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.Date;

/**
 * This class represents the statistics regardings a job
 * Some examples are throughput, source, destination, time started, time completed more to come.
 */
@Data
@AllArgsConstructor
public class JobStatistics {
    int job_execution_id;
    Date start_time;
    Date end_time;
    Status status;
    Date last_updated;


}
