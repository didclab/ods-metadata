package org.onedatashare.odsmetadata.model;

import lombok.Data;

/**
 * This class represents the statistics regardings a job
 * Some examples are throughput, source, destination, time started, time completed more to come.
 */
@Data
public class JobStatistics {
    Status status;
}
