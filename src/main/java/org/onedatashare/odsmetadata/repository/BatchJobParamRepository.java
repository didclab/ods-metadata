package org.onedatashare.odsmetadata.repository;

import org.onedatashare.odsmetadata.entity.BatchJobExecutionParams;
import org.onedatashare.odsmetadata.model.BatchJobData;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BatchJobParamRepository extends JpaRepository<BatchJobExecutionParams,Long> {

    List<BatchJobExecutionParams> findBatchJobExecutionParamsByJobExecutionId(Long id);

    List<BatchJobExecutionParams> findBatchJobExecutionParamsByStringValLike(String userId);
}
