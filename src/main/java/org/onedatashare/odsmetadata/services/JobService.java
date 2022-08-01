package org.onedatashare.odsmetadata.services;

import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.mapstruct.factory.Mappers;
import org.onedatashare.odsmetadata.entity.BatchJobExecution;
import org.onedatashare.odsmetadata.entity.BatchJobExecutionParams;
import org.onedatashare.odsmetadata.mapper.BatchJobMapper;
import org.onedatashare.odsmetadata.model.BatchJobData;
import org.onedatashare.odsmetadata.repository.BatchJobParamRepository;
import org.onedatashare.odsmetadata.repository.BatchJobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class JobService {

    @Autowired
    BatchJobRepository batchJobRepository;

    @Autowired
    BatchJobParamRepository batchJobParamRepository;

    private BatchJobMapper mapper = Mappers.getMapper(BatchJobMapper.class);

    private static final String STRING_VAL = "STRING";
    private static final String LONG_VAL = "LONG";
    private static final String DOUBLE_VAL = "DOUBLE";

    public BatchJobData getJobStat(String jobId){
        log.info(jobId);
        BatchJobData batchJobData = new BatchJobData();
        BatchJobExecution batchJobExecution = batchJobRepository.findBatchJobExecutionById(Long.valueOf(jobId));
        batchJobData = mapper.mapBatchJobEntityToModel(batchJobExecution);
        List<BatchJobExecutionParams> batchJobExecutionParams = batchJobParamRepository.findBatchJobExecutionParamsByJobExecutionId(Long.valueOf(jobId));
        batchJobData.setJobParameters(mapJobParameters(batchJobExecutionParams));
        return batchJobData;
    }

    public List<Long> getUserJobIds(String userId){
        log.info(userId);
        List<BatchJobExecutionParams> batchJobExecutionParams = batchJobParamRepository.findBatchJobExecutionParamsByStringValLike(userId);
        List<Long> userJobIds = batchJobExecutionParams.stream().map(BatchJobExecutionParams :: getJobExecutionId).collect(Collectors.toList());
        return userJobIds;
    }


    public List<BatchJobData> getAllJobStatisticsOfUser(String userId){
        log.info(userId);
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findAllByBatchJobParams_StringValLike(userId);
        batchJobExecutions.stream().forEach(batchJobExecution -> {
            BatchJobData batchJobData = new BatchJobData();
            batchJobData = mapper.mapBatchJobEntityToModel(batchJobExecution);
            batchJobData.setJobParameters(mapJobParameters(batchJobExecution.getBatchJobParams()));
            batchJobDataList.add(batchJobData);
        });
        log.info("Total jobs for user:"+batchJobDataList.size());
        return batchJobDataList;
    }
    public BatchJobData getUserJobsByDate(String userId, Date date){
        BatchJobData batchJobData = new BatchJobData();
        BatchJobExecution batchJobExecution = batchJobRepository.findByStartTimeAndBatchJobParams_StringVal(new Timestamp(date.getTime()),userId);
        batchJobData = mapper.mapBatchJobEntityToModel(batchJobExecution);
        batchJobData.setJobParameters(mapJobParameters(batchJobExecution.getBatchJobParams()));
        return batchJobData;
    }

    public List<Long> getUserJobsByDateRange(String userId, Date from, Date to){
        List<BatchJobExecution> batchJobExecution = batchJobRepository.findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_StringValLike(from,to,userId);
        List<Long> jobIds = batchJobExecution.stream().map(BatchJobExecution::getId).collect(Collectors.toList());
        return jobIds;
    }

    @NotNull
    private Map<String, String> mapJobParameters(List<BatchJobExecutionParams> batchJobExecutionParams) {
        Map<String,String> jobParameters = new HashMap<>();
        for( BatchJobExecutionParams job : batchJobExecutionParams) {
            if(STRING_VAL.equals(job.getTypeCd())) {
                jobParameters.put(job.getKeyName(),job.getStringVal());
            }else if(LONG_VAL.equals(job.getTypeCd())) {
                jobParameters.put(job.getKeyName(),job.getLongVal().toString());
            } else if (DOUBLE_VAL.equals(job.getTypeCd())) {
                jobParameters.put(job.getKeyName(),job.getDoubleVal().toString());
            }
        }
        return jobParameters;
    }
}
