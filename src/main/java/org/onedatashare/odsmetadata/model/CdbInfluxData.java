package org.onedatashare.odsmetadata.model;

import lombok.Data;

import java.util.List;

@Data
public class CdbInfluxData {
    List<InfluxData> measurements;
    JobStatistic jobStatistic;
}
