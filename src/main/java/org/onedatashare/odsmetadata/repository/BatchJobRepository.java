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

    BatchJobExecution findByStartTimeAndBatchJobParams_ParameterValueLike(Date date, String userId);

    //Make Pageable
    List<BatchJobExecution> findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_ParameterValueLike(Date startTime, Date endTime, String userId);

    List<BatchJobExecution> findAllByBatchJobParams_ParameterValueLike(String userId);

    //Pageable version
    Page<BatchJobExecution> findAllByBatchJobParams_ParameterValueLike(String userId, Pageable pr);

    List<BatchJobExecution> findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_ParameterValueLike(Date startTime, Date endTime, String userId, Pageable pr);
}
