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
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class JobService {

    private BatchJobRepository batchJobRepository;

    private BatchJobParamRepository batchJobParamRepository;

    private BatchJobMapper mapper = Mappers.getMapper(BatchJobMapper.class);

    private static final String STRING_VAL = "STRING";
    private static final String LONG_VAL = "LONG";
    private static final String DOUBLE_VAL = "DOUBLE";

    @Autowired
    public JobService(BatchJobParamRepository batchJobParamRepository, BatchJobRepository batchJobRepository) {
        this.batchJobParamRepository = batchJobParamRepository;
        this.batchJobRepository = batchJobRepository;
    }

    public BatchJobData getJobStat(Long jobId) {
        log.info("JobId: {}", jobId);
        BatchJobData batchJobData = new BatchJobData();
        BatchJobExecution batchJobExecution = batchJobRepository.findBatchJobExecutionById(jobId);
        if (Objects.nonNull(batchJobExecution)) {
            batchJobData = mapper.mapBatchJobEntityToModel(batchJobExecution);
            List<BatchJobExecutionParams> batchJobExecutionParams = batchJobParamRepository.findBatchJobExecutionParamsByJobExecutionId(jobId);
            if (!CollectionUtils.isEmpty(batchJobExecution.getBatchJobParams())) {
                batchJobData.setJobParameters(mapJobParameters(batchJobExecution.getBatchJobParams()));
            }
        }
        return batchJobData;
    }

    public List<Long> getUserJobIds(String userId) {
        log.info(userId);
        List<Long> userJobIds = new ArrayList<>();
        List<BatchJobExecutionParams> batchJobExecutionParams = batchJobParamRepository.findBatchJobExecutionParamsByStringValLike(userId);
        if (!CollectionUtils.isEmpty(batchJobExecutionParams)) {
            userJobIds = batchJobExecutionParams.stream().map(BatchJobExecutionParams::getJobExecutionId).collect(Collectors.toList());
        }
        return userJobIds;
    }


    public List<BatchJobData> getAllJobStatisticsOfUser(String userId) {
        log.info(userId);
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findAllByBatchJobParams_StringValLike(userId);
        processBatchJobExecutionData(batchJobDataList, batchJobExecutions);
        log.info("Total jobs for user:" + batchJobDataList.size());
        return batchJobDataList;
    }

    public Page<BatchJobData> getAllJobStatisticsOfUser(String userId, Pageable pr) {
        log.info("UserId={}, Pageable={}", userId, pr);
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findAllByBatchJobParams_StringValLike(userId, pr);
        processBatchJobExecutionData(batchJobDataList, batchJobExecutions);
        log.info("Total jobs for user:" + batchJobDataList.size());
        return new PageImpl<BatchJobData>(batchJobDataList, pr, batchJobDataList.size());
    }

    public BatchJobData getUserJobsByDate(String userId, Instant date) {
        log.info(date.toString());
        log.info(Date.from(date).toString());
        BatchJobData batchJobData = new BatchJobData();
        BatchJobExecution batchJobExecution = batchJobRepository.findByStartTimeAndBatchJobParams_StringVal(Date.from(date), userId);
        if (Objects.nonNull(batchJobExecution)) {
            batchJobData = mapper.mapBatchJobEntityToModel(batchJobExecution);
            if (!CollectionUtils.isEmpty(batchJobExecution.getBatchJobParams())) {
                batchJobData.setJobParameters(mapJobParameters(batchJobExecution.getBatchJobParams()));
            }
        }
        return batchJobData;
    }

    public List<BatchJobData> getUserJobsByDateRange(String userId, Instant from, Instant to) {
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_StringValLike(Date.from(from), Date.from(to), userId);
        processBatchJobExecutionData(batchJobDataList, batchJobExecutions);
        return batchJobDataList;
    }

    public Page<BatchJobData> getUserJobsByDateRange(String userId, Instant from, Instant to, Pageable pr) {
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_StringValLike(Date.from(from), Date.from(to), userId, pr);
        processBatchJobExecutionData(batchJobDataList, batchJobExecutions);
        return new PageImpl<BatchJobData>(batchJobDataList, pr, batchJobDataList.size());
    }

    private void processBatchJobExecutionData(List<BatchJobData> batchJobDataList, List<BatchJobExecution> batchJobExecutions) {
        if (!CollectionUtils.isEmpty(batchJobExecutions)) {
            batchJobExecutions.stream().forEach(batchJobExecution -> {
                BatchJobData batchJobData = new BatchJobData();
                batchJobData = mapper.mapBatchJobEntityToModel(batchJobExecution);
                if (!CollectionUtils.isEmpty(batchJobExecution.getBatchJobParams())) {
                    batchJobData.setJobParameters(mapJobParameters(batchJobExecution.getBatchJobParams()));
                }
                batchJobDataList.add(batchJobData);
            });
        }
    }

    @NotNull
    private Map<String, String> mapJobParameters(List<BatchJobExecutionParams> batchJobExecutionParams) {
        Map<String, String> jobParameters = new HashMap<>();
        for (BatchJobExecutionParams job : batchJobExecutionParams) {
            if (STRING_VAL.equals(job.getTypeCd())) {
                jobParameters.put(job.getKeyName(), job.getStringVal());
            } else if (LONG_VAL.equals(job.getTypeCd())) {
                jobParameters.put(job.getKeyName(), job.getLongVal().toString());
            } else if (DOUBLE_VAL.equals(job.getTypeCd())) {
                jobParameters.put(job.getKeyName(), job.getDoubleVal().toString());
            }
        }
        return jobParameters;
    }
}
