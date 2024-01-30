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

    BatchJobRepository batchJobRepository;

    BatchJobParamRepository batchJobParamRepository;

    private BatchJobMapper mapper;

    public JobService(BatchJobRepository batchJobRepository, BatchJobParamRepository batchJobParamRepository) {
        this.batchJobRepository = batchJobRepository;
        this.batchJobParamRepository = batchJobParamRepository;
        this.mapper = Mappers.getMapper(BatchJobMapper.class);
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
        List<BatchJobExecutionParams> batchJobExecutionParams = batchJobParamRepository.findBatchJobExecutionParamsByParameterValueLike(userId);
        if (!CollectionUtils.isEmpty(batchJobExecutionParams)) {
            userJobIds = batchJobExecutionParams.stream().map(BatchJobExecutionParams::getJobExecutionId).collect(Collectors.toList());
        }
        return userJobIds;
    }

    public List<UUID> getUserUuids(String userEmail) {
        List<UUID> jobUuids = new ArrayList<>();
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findAllByBatchJobParams_ParameterValueLike(userEmail);
        processBatchJobExecutionData(batchJobDataList, batchJobExecutions);
        for (BatchJobData data : batchJobDataList) {
            String jobUuid = data.getJobParameters().get("jobUuid");
            if (jobUuid != null) {
                jobUuids.add(UUID.fromString(jobUuid));
            }
        }
        log.info("Total jobs for user:" + batchJobDataList.size());
        return jobUuids;
    }


    public List<BatchJobData> getAllJobStatisticsOfUser(String userId) {
        log.info(userId);
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findAllByBatchJobParams_ParameterValueLike(userId);
        processBatchJobExecutionData(batchJobDataList, batchJobExecutions);
        log.info("Total jobs for user:" + batchJobDataList.size());
        return batchJobDataList;
    }

    public Page<BatchJobData> getAllJobStatisticsOfUser(String userId, Pageable pr) {
        log.info("UserId={}, Pageable={}", userId, pr);
        Page<BatchJobExecution> page = batchJobRepository.findAllByBatchJobParams_ParameterValueLike(userId, pr);
        List<BatchJobData> batchJobData = new ArrayList<>();
        processBatchJobExecutionData(batchJobData, page.getContent());
        return new PageImpl<>(batchJobData, page.getPageable(), page.getTotalElements());
    }

    public BatchJobData getUserJobsByDate(String userId, Instant date) {
        log.info(date.toString());
        log.info(Date.from(date).toString());
        BatchJobData batchJobData = new BatchJobData();
        BatchJobExecution batchJobExecution = batchJobRepository.findByStartTimeAndBatchJobParams_ParameterValueLike(Date.from(date), userId);
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
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_ParameterValueLike(Date.from(from), Date.from(to), userId);
        processBatchJobExecutionData(batchJobDataList, batchJobExecutions);
        return batchJobDataList;
    }

    public List<BatchJobData> getBatchDataFromUuids(UUID jobUuid) {
        List<BatchJobData> retList = new ArrayList<>();
        List<BatchJobExecution> executions = new ArrayList<>();
        List<BatchJobExecutionParams> params = batchJobParamRepository.findBatchJobExecutionParamsByParameterValueLike(jobUuid.toString());
        executions = params.stream().map(param -> {
            return this.batchJobRepository.findBatchJobExecutionById(param.getJobExecutionId());
        }).collect(Collectors.toList());
        processBatchJobExecutionData(retList, executions);
        return retList;
    }


    public Page<BatchJobData> getUserJobsByDateRange(String userId, Instant from, Instant to, Pageable pr) {
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_ParameterValueLike(Date.from(from), Date.from(to), userId, pr);
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
            jobParameters.put(job.getParameterName(), job.getParameterValue());
        }
        return jobParameters;
    }

}
