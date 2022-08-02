package org.onedatashare.odsmetadata.repository;

import org.onedatashare.odsmetadata.entity.BatchJobExecution;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;

@Repository
public interface BatchJobRepository extends CrudRepository<BatchJobExecution, Long> {


    BatchJobExecution findBatchJobExecutionById(Long id);
    BatchJobExecution findByStartTimeAndBatchJobParams_StringVal(Date date, String userId);

    List<BatchJobExecution> findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_StringValLike(Date startTime, Date endTime, String userId);

    List<BatchJobExecution> findAllByBatchJobParams_StringValLike(String userId);

}
