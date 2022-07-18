package org.onedatashare.odsmetadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class JobParamDetails {

    String time;
    String ownerId;
    String priority;
    String chunkSize;
    String sourcePath;
    String destPath;
    String sourceCreds;
    String destCreds;
    String sourceType;
    String destType;
    String compress;
    String concurrency;
    String pipelining;
    String parallelism;
    String retry;
    String fileDetails;
    
}
