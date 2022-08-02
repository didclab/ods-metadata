package org.onedatashare.odsmetadata.mapper;

import org.mapstruct.Mapper;
import org.onedatashare.odsmetadata.entity.BatchJobExecution;
import org.onedatashare.odsmetadata.model.BatchJobData;

@Mapper(componentModel = "spring")
public interface BatchJobMapper {

    BatchJobData mapBatchJobEntityToModel(BatchJobExecution batchJobExecution);
}
