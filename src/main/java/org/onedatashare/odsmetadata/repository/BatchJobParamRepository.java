package org.onedatashare.odsmetadata.repository;

import org.onedatashare.odsmetadata.entity.BatchJobExecutionParams;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BatchJobParamRepository extends CrudRepository<BatchJobExecutionParams,Long> {

    List<BatchJobExecutionParams> findBatchJobExecutionParamsByJobExecutionId(Long id);

    List<BatchJobExecutionParams> findBatchJobExecutionParamsByStringValLike(String userId);
}
