package org.onedatashare.odsmetadata.entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import jakarta.persistence.*;
import java.sql.Timestamp;
@Data
@Entity
@Table(name="batch_step_execution")
public class BatchStepExecution {

    @Id
    @Column(name="step_execution_id")
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "job_execution_id" ,insertable = false, updatable = false)
    @JsonIgnore
    private BatchJobExecution batchJob;

    @Column(name="version")
    private Long version;

    @Column(name="step_name")
    private String step_name;

    @Column(name="job_execution_id")
    private Long jobInstanceId;

    @Column(name="start_time")
    private Timestamp startTime;

    @Column(name="end_time")
    private Timestamp endTime;

    @Column(name="status")
    private String status;

    @Column(name="commit_count")
    private Long commitCount;

    @Column(name="read_count")
    private Long readCount;

    @Column(name="filter_count")
    private Long filterCount;

    @Column(name="write_count")
    private Long writeCount;

    @Column(name="read_skip_count")
    private Long readSkipcount;

    @Column(name="write_skip_count")
    private Long writeSkipCount;

    @Column(name="process_skip_count")
    private Long processSkipCount;

    @Column(name="rollback_count")
    private Long rollbackCount;

    @Column(name="exit_code")
    private String exitCode;

    @Column(name="exit_message")
    private String exitMessage;

    @Column(name="last_updated")
    private Timestamp lastUpdated;

}
