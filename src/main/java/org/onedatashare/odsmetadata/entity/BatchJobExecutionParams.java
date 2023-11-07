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

    @Column(name="parameter_type")
    private String parameterType;

    @Column(name="parameter_name")
    private String parameterName;

    @Column(name="parameter_value")
    private String parameterValue;

    @Column(name="identifying")
    private Character identifying;

    @Override
    public String toString(){
        return this.parameterName + ":" + this.parameterValue;
    }
}