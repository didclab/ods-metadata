package org.onedatashare.odsmetadata.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Measurement(name = "transfer_data")
@JsonIgnoreProperties(ignoreUnknown = true)
public class InfluxData {

    @Column(name = "interface")
    private String networkInterface;

    @Column(name = "ods_user")
    private String odsUser;

    @Column(name = "transfer_node_name")
    private String transferNodeName;

    @Column(name = "active_core_count")
    private Double coreCount;

    @Column(name = "cpu_frequency_max")
    private Double cpu_frequency_max;

    @Column(name = "cpu_frequency_current")
    private Double cpu_frequency_current;

    @Column(name = "cpu_frequency_min")
    private Double cpu_frequency_min;

    @Column(name = "energy_consumed")
    private Double energyConsumed;

    @Column(name = "cpu_arch")
    private String cpuArchitecture;

    @Column(name = "packet_loss_rate")
    private Double packetLossRate;

    @Column(name = "link_capacity")
    private Double linkCapacity;

    /* Delta values*/
    @Column(name = "bytes_sent_delta")
    private Long bytesSentDelta;

    @Column(name = "bytes_received_delta")
    private Long bytesReceivedDelta;

    @Column(name = "packets_sent_delta")
    private Long packetsSentDelta;

    @Column(name = "packets_received_delta")
    private Long packetsReceivedDelta;

    //NIC values

    @Column(name = "bytes_sent")
    private Long bytesSent;

    @Column(name = "bytes_recv")
    private Long bytesReceived;

    @Column(name = "packets_sent")
    private Long packetSent;

    @Column(name = "packets_recv")
    private Long packetReceived;

    @Column(name = "dropin")
    private Double dropin;

    @Column(name = "dropout")
    private Double dropout;

    @Column(name = "nic_speed")
    private Double nicSpeed;

    @Column(name = "nic_mtu")
    private Double nicMtu;

    //2022-06-01 10:41:15.123591
    @Column(name = "start_time")
    private LocalDateTime startTime;

    @Column(name = "end_time")
    private LocalDateTime endTime;

    @Column(name = "latency")
    private Double latency;

    @Column(name = "rtt")
    private Double rtt;

    @Column(name = "errin")
    private Double errin;

    @Column(name = "errout")
    private Double errout;

    //Job Values

    @Column(name = "jobId")
    private Long jobId;

    @Column(name = "throughput")
    private Double throughput;

    @Column(name = "concurrency")
    private Long concurrency;

    @Column(name = "parallelism")
    private Long parallelism;

    @Column(name = "pipelining")
    private Long pipelining;

    @Column(name = "memory")
    private Long memory;

    @Column(name = "maxMemory")
    private Long maxMemory;

    @Column(name = "freeMemory")
    private Long freeMemory;

    @Column(name = "jobSize")
    private Long jobSize;

    @Column(name = "avgJobSize")
    private Long avgFileSize;

    @Column(name = "totalBytesSent")
    private Long dataBytesSent;

    @Column(name = "compression")
    private Boolean compression;

    @Column(name = "allocatedMemory")
    private Long allocatedMemory;
}
