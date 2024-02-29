package org.onedatashare.odsmetadata.repository;

import org.onedatashare.odsmetadata.entity.BatchJobExecution;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public interface BatchJobRepository extends PagingAndSortingRepository<BatchJobExecution, Long> {


    BatchJobExecution findBatchJobExecutionById(Long id);

    Page<BatchJobExecution> findByStartTimeAndBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(Date date, String parameterName, String userId, Pageable pg);

    //Make Pageable
    List<BatchJobExecution> findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(Date startTime, Date endTime, String ParameterName, String userId);

    List<BatchJobExecution> findAllByBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(String parameterName, String userId);

    //Pageable version
    Page<BatchJobExecution> findAllByBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(String parameterName, String userId, Pageable pr);

    Page<BatchJobExecution> findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_ParameterNameAndBatchJobParams_ParameterValueLike(Date startTime, Date endTime, String parameterName, String userId, Pageable pr);

    List<BatchJobExecution> findAllByBatchJobParams_ParameterNameLikeAndBatchJobParams_ParameterValueLikeAndStatus(String parameterName, String userId, String status);
}
