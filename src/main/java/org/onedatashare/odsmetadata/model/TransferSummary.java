package org.onedatashare.odsmetadata.model;

import lombok.Data;

import java.util.DoubleSummaryStatistics;
import java.util.List;

@Data
public class TransferSummary {

    long bytesRead;
    long bytesWritten;
    double averageReadThroughput;
    double averageWriteThroughput;
    double progressPercentage;
    long jobSize;
    String transferStatus;

    public void updateSummary(List<InfluxData> jobMeasurements) {
        DoubleSummaryStatistics readStat = new DoubleSummaryStatistics();
        DoubleSummaryStatistics writeStat = new DoubleSummaryStatistics();
        for (InfluxData measurement : jobMeasurements) {
            this.bytesRead += measurement.getBytesRead();
            this.bytesWritten += measurement.getBytesWritten();
            readStat.accept(measurement.getReadThroughput());
            writeStat.accept(measurement.getWriteThroughput());
            this.jobSize = measurement.getJobSize();
        }
        this.averageReadThroughput = readStat.getAverage();
        this.averageWriteThroughput = writeStat.getAverage();
        this.progressPercentage = ((double) this.bytesWritten / this.jobSize) * 100.0;
    }
}
