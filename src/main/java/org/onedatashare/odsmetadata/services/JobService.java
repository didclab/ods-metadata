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

    public List<Long> getUserJobIds(String userId) {
        return batchJobParamRepository.findBatchJobExecutionParamsByParameterNameAndParameterValueLike(emailParameterName, userId)
                .stream()
                .map(BatchJobExecutionParams::getJobExecutionId)
                .collect(Collectors.toList());
    }

    public List<UUID> getUserUuids(String userEmail) {
        List<UUID> jobUuids = new ArrayList<>();
        List<BatchJobData> batchJobDataList = new ArrayList<>();
        List<BatchJobExecution> batchJobExecutions = batchJobRepository.findAllByBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(emailParameterName, userEmail);
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

    //pageable version
    public BatchJobData getBatchDataFromUuids(UUID jobUuid) {
        BatchJobExecutionParams jobParam = batchJobParamRepository.findBatchJobExecutionParamsByParameterNameLikeAndParameterValueLike(jobUuidParameterName, jobUuid.toString());
        BatchJobExecution jobData = batchJobRepository.findBatchJobExecutionById(jobParam.getJobExecutionId());
        BatchJobData batchJobData = mapper.mapBatchJobEntityToModel(jobData);
        if (!CollectionUtils.isEmpty(jobData.getBatchJobParams())) {
            batchJobData.setJobParameters(mapJobParameters(jobData.getBatchJobParams()));
        }
        return batchJobData;
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
