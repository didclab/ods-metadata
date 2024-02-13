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
import org.springframework.data.domain.PageRequest;
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

    private final String emailParameterName = "ownerId";

    private final String jobUuidParameterName = "jobUuid";

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

    public Page<Long> getUserJobIds(String userId, Pageable pg) {
        List<Long> userJobIds = new ArrayList<>();
        Page<BatchJobExecutionParams> page = batchJobParamRepository.findBatchJobExecutionParamsByParameterNameAndParameterValueLike(emailParameterName, userId, pg);
        if (!CollectionUtils.isEmpty(page.getContent())) {
            userJobIds = page.getContent().stream().map(BatchJobExecutionParams::getJobExecutionId).collect(Collectors.toList());
        }
        return new PageImpl<>(userJobIds, page.getPageable(), userJobIds.size());
    }

    public Page<UUID> getUserUuids(String userEmail, Pageable pg) {
        List<UUID> jobUuids = new ArrayList<>();
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        Page<BatchJobExecution> page = batchJobRepository.findAllByBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(emailParameterName, userEmail, pg);
        processBatchJobExecutionData(batchJobDataList, page.getContent());
        for (BatchJobData data : batchJobDataList) {
            String jobUuid = data.getJobParameters().get("jobUuid");
            if (jobUuid != null) {
                jobUuids.add(UUID.fromString(jobUuid));
            }
        }
        log.info("Total jobs for user:" + batchJobDataList.size());
        return new PageImpl<>(jobUuids, page.getPageable(), jobUuids.size());
    }


    public List<BatchJobData> getAllJobStatisticsOfUser(String userId) {
        log.info(userId);
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findAllByBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(emailParameterName, userId);
        processBatchJobExecutionData(batchJobDataList, batchJobExecutions);
        log.info("Total jobs for user:" + batchJobDataList.size());
        return batchJobDataList;
    }

    public Page<BatchJobData> getAllJobStatisticsOfUser(String userId, Pageable pr) {
        Page<BatchJobExecution> page = batchJobRepository.findAllByBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(emailParameterName, userId, pr);
        List<BatchJobData> batchJobData = new ArrayList<>();
        processBatchJobExecutionData(batchJobData, page.getContent());
        return new PageImpl<>(batchJobData, page.getPageable(), page.getTotalElements());
    }

    public Page<BatchJobData> getUserJobsByDate(String userId, Instant date, Pageable pg) {
        log.info(date.toString());
        log.info(Date.from(date).toString());
        List<BatchJobData> batchJobDatalist = new ArrayList<>();
        Page<BatchJobExecution> page = batchJobRepository.findByStartTimeAndBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(Date.from(date), emailParameterName, userId, pg);
        if (Objects.nonNull(page)) {
            page.getContent().stream().forEach(batchJobExecution -> {
                BatchJobData batchJobData = mapper.mapBatchJobEntityToModel(batchJobExecution);
                batchJobData.setJobParameters(mapJobParameters(batchJobExecution.getBatchJobParams()));
                batchJobDatalist.add(batchJobData);
            });
        }
        return new PageImpl<>(batchJobDatalist, page.getPageable(), batchJobDatalist.size());
    }

    public List<BatchJobData> getUserJobsByDateRange(String userId, Instant from, Instant to) {
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(Date.from(from), Date.from(to), emailParameterName, userId);
        processBatchJobExecutionData(batchJobDataList, batchJobExecutions);
        return batchJobDataList;
    }

    public List<BatchJobData> getBatchDataFromUuids(UUID jobUuid) {
        List<BatchJobData> batchJobData = new ArrayList<>();
        List<BatchJobExecution> executions = new ArrayList<>();
        List<BatchJobExecutionParams> batchJobExecutionParams = batchJobParamRepository.findBatchJobExecutionParamsByParameterNameAndParameterValueLike(jobUuidParameterName, jobUuid.toString());
        executions = batchJobExecutionParams.stream().map(param -> {
            return this.batchJobRepository.findBatchJobExecutionById(param.getJobExecutionId());
        }).collect(Collectors.toList());
        processBatchJobExecutionData(batchJobData, executions);
        return batchJobData;
    }

    //pageable version
    public Page<BatchJobData> getBatchDataFromUuids(UUID jobUuid, Pageable pg) {
        List<BatchJobData> batchJobData = new ArrayList<>();
        List<BatchJobExecution> executions = new ArrayList<>();
        Page<BatchJobExecutionParams> page = batchJobParamRepository.findBatchJobExecutionParamsByParameterNameAndParameterValueLike(jobUuidParameterName, jobUuid.toString(), pg);
        executions = page.getContent().stream().map(param -> {
            return this.batchJobRepository.findBatchJobExecutionById(param.getJobExecutionId());
        }).collect(Collectors.toList());
        processBatchJobExecutionData(batchJobData, executions);
        return new PageImpl<>(batchJobData, page.getPageable(), batchJobData.size());
    }


    public Page<BatchJobData> getUserJobsByDateRange(String userId, Instant from, Instant to, Pageable pr) {
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        Page<BatchJobExecution> page = batchJobRepository.findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(Date.from(from), Date.from(to), emailParameterName, userId, pr);
        processBatchJobExecutionData(batchJobDataList, page.getContent());
        return new PageImpl<BatchJobData>(batchJobDataList, page.getPageable(), batchJobDataList.size());
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
