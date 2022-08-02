package org.onedatashare.odsmetadata.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import javax.persistence.*;
import java.sql.Timestamp;
@Data
@Entity
@Table(name="batch_job_execution_params")
public class BatchJobExecutionParams {

    @Id
    private Long rowid;

    @Column(name="job_execution_id")
    private Long jobExecutionId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_execution_id" ,insertable = false, updatable = false)
    @JsonIgnore
    private BatchJobExecution batchJob;

    @Column(name="type_cd")
    private String typeCd;

    @Column(name="key_name")
    private String keyName;

    @Column(name="string_val")
    private String stringVal;

    @Column(name="date_val")
    private Timestamp dateVal;

    @Column(name="long_val")
    private Long longVal;

    @Column(name="double_val")
    private Float doubleVal;

    @Column(name="identifying")
    private Character identifying;
}