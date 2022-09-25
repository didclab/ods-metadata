package org.onedatashare.odsmetadata.repository;

import org.onedatashare.odsmetadata.entity.BatchJobExecution;
import org.onedatashare.odsmetadata.entity.BatchJobExecutionParams;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public interface BatchJobRepository extends PagingAndSortingRepository<BatchJobExecution, Long> {


    BatchJobExecution findBatchJobExecutionById(Long id);

    BatchJobExecution findByStartTimeAndBatchJobParams_StringVal(Date date, String userId);

    //Make Pageable
    List<BatchJobExecution> findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_StringValLike(Date startTime, Date endTime, String userId);

    List<BatchJobExecution> findAllByBatchJobParams_StringValLike(String userId);

    //Pageable version
    List<BatchJobExecution> findAllByBatchJobParams_StringValLike(String userId, Pageable pr);

    List<BatchJobExecution> findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_StringValLike(Date startTime, Date endTime, String userId, Pageable pr);
}
