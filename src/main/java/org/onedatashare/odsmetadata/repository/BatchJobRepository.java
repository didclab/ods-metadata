package org.onedatashare.odsmetadata.repository;

import org.onedatashare.odsmetadata.entity.BatchJobExecution;
import org.onedatashare.odsmetadata.entity.BatchJobExecutionParams;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;
import org.springframework.data.repository.query.Param;
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
    List<BatchJobExecution> findAllByBatchJobParams_ParameterValueLike(String userId, Pageable pr);

    List<BatchJobExecution> findByStartTimeIsGreaterThanEqualAndEndTimeIsLessThanEqualAndBatchJobParams_ParameterValueLike(Date startTime, Date endTime, String userId, Pageable pr);

    @Query("SELECT DISTINCT j FROM BatchJobExecution j " +
            "INNER JOIN j.batchJobParams p " +
            "INNER JOIN j.batchJobParams p2 " +
            "WHERE ((p.parameterName = 'sourceCredentialType' AND p.parameterValue LIKE %:type%) " +
            "OR (p.parameterName = 'destCredentialType' AND p.parameterValue LIKE %:type%)) " +
            "AND p2.parameterValue LIKE %:email%")
    List<BatchJobExecution> findBatchJobExecutionByEmailAndSourceAndDestinationType(
            @Param("email") String email, @Param("type") String type);
}
