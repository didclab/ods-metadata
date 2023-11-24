package org.onedatashare.odsmetadata.entity;

import lombok.Data;

import jakarta.persistence.*;
import java.sql.Timestamp;
import java.util.List;

@Data
@Entity
@Table(name="batch_job_execution")
public class BatchJobExecution {

    @Id
    @Column(name="job_execution_id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name="version")
    private Long version;

    @Column(name="job_instance_id")
    private Long jobInstanceId;

    @Column(name="create_time")
    private Timestamp createTime;

    @Column(name="start_time")
    private Timestamp startTime;

    @Column(name="end_time")
    private Timestamp endTime;

    @Column(name="status")
    private String status;

    @Column(name="exit_code")
    private String exitCode;

    @Column(name="exit_message")
    private String exitMessage;

    @Column(name="last_updated")
    private Timestamp lastUpdated;

    @OneToMany(mappedBy = "batchJob")
    List<BatchStepExecution> batchSteps;

    @OneToMany(mappedBy = "batchJob")
    List<BatchJobExecutionParams> batchJobParams;
}
