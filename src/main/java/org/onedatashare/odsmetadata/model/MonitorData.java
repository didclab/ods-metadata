package org.onedatashare.odsmetadata.model;

import lombok.Data;

import java.util.List;

@Data
public class MonitorData {
    List<BatchJobData> jobData;
    List<InfluxData> measurementData;
}
