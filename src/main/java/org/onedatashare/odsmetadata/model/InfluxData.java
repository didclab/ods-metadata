package org.onedatashare.odsmetadata.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Measurement(name = "transfer_data")
public class InfluxData {

    @Column(name = NETWORK_INTERFACE)
    private String networkInterface;

    @Column(name = ODS_USER, tag = true)
    private String odsUser;

    @Column(name = TRANSFER_NODE_NAME, tag = true)
    private String transferNodeName;

    @Column(name = ACTIVE_CORE_COUNT)
    private Long coreCount;

    @Column(name = CPU_FREQUENCY_MAX)
    private Double cpu_frequency_max;

    @Column(name = CPU_FREQUENCY_CURRENT)
    private Double cpu_frequency_current;

    @Column(name = CPU_FREQUENCY_MIN)
    private Double cpu_frequency_min;

    @Column(name = CPU_ARCHITECTURE)
    private String cpuArchitecture;

    @Column(name = PACKET_LOSS_RATE)
    private Double packetLossRate;
    //NIC values
    @Column(name = BYTES_SENT)
    private Long bytesSent;

    @Column(name = BYTES_RECEIVED)
    private Long bytesReceived;

    @Column(name = PACKETS_SENT)
    private Long packetSent;

    @Column(name = PACKETS_RECEIVED)
    private Long packetReceived;

    @Column(name = DROP_IN)
    private Long dropin;

    @Column(name = DROP_OUT)
    private Long dropout;

    @Column(name = NIC_MTU)
    private Long nicMtu;

    @Column(name = LATENCY)
    private Double latency;

    @Column(name = RTT)
    private Double rtt;

    @Column(name = SOURCE_RTT)
    private Double sourceRtt;

    @Column(name = SOURCE_LATENCY)
    private Double sourceLatency;

    @Column(name = DESTINATION_RTT)
    private Double destinationRtt;

    @Column(name = DEST_LATENCY)
    private Double destLatency;

    @Column(name = ERROR_IN)
    private Long errin;

    @Column(name = ERROR_OUT)
    private Long errout;

    //Job Values
    @Column(name = JOB_ID, tag = true)
    private String jobId;
    @Column(name = READ_THROUGHPUT)
    private Double readThroughput;
    @Column(name = WRITE_THROUGHPUT)
    private Double writeThroughput;
    @Column(name = BYTES_UPLOADED)
    private Long bytesWritten;
    @Column(name = BYTES_DOWNLOADED)
    private Long bytesRead;
    @Column(name = CONCURRENCY)
    private Long concurrency;

    @Column(name = PARALLELISM)
    private Long parallelism;
    @Column(name = PIPELINING)
    private Long pipelining;
    @Column(name = MEMORY)
    private Long memory;
    @Column(name = MAX_MEMORY)
    private Long maxMemory;
    @Column(name = FREE_MEMORY)
    private Long freeMemory;
    @Column(name = ALLOCATED_MEMORY)
    private Long allocatedMemory;
    @Column(name = JOB_SIZE)
    private Long jobSize;
    @Column(name = AVERAGE_FILE_SIZE)
    private Long avgFileSize;

    @Column(name = SOURCE_TYPE, tag = true)
    private String sourceType;
    @Column(name = SOURCE_CRED_ID, tag = true)
    private String sourceCredId;

    @Column(name = DESTINATION_TYPE, tag = true)
    private String destType;
    @Column(name = DESTINATION_CRED_IT, tag = true)
    private String destCredId;

    //constant keys copied from Transfer-Service DataInfluxConstants.java
    public static final String NETWORK_INTERFACE = "interface";
    public static final String ODS_USER = "ods_user";
    public static final String TRANSFER_NODE_NAME = "transfer_node_name";
    public static final String ACTIVE_CORE_COUNT = "active_core_count";
    public static final String CPU_FREQUENCY_MAX = "cpu_frequency_max";
    public static final String CPU_FREQUENCY_CURRENT = "cpu_frequency_current";
    public static final String CPU_FREQUENCY_MIN = "cpu_frequency_min";
    public static final String CPU_ARCHITECTURE = "cpu_arch";
    public static final String PACKET_LOSS_RATE = "packet_loss_rate";
    public static final String BYTES_DOWNLOADED = "bytesDownloaded";
    public static final String BYTES_UPLOADED = "bytesUploaded";

    public static final String BYTES_SENT = "bytes_sent";
    public static final String BYTES_RECEIVED = "bytes_recv";
    public static final String PACKETS_SENT = "packets_sent";
    public static final String PACKETS_RECEIVED = "packets_recv";
    public static final String DROP_IN = "dropin";
    public static final String DROP_OUT = "dropout";
    public static final String NIC_MTU = "nic_mtu";
    public static final String LATENCY = "latency";
    public static final String RTT = "rtt";
    public static final String SOURCE_RTT = "source_rtt";
    public static final String SOURCE_LATENCY = "source_latency";

    public static final String DEST_LATENCY = "destination_latency";
    public static final String DESTINATION_RTT = "destination_rtt";
    public static final String ERROR_IN = "errin";
    public static final String ERROR_OUT = "errout";
    public static final String JOB_ID = "jobId";
    public static final String CONCURRENCY = "concurrency";
    public static final String PARALLELISM = "parallelism";
    public static final String PIPELINING = "pipelining";
    public static final String MEMORY = "memory";
    public static final String MAX_MEMORY = "maxMemory";
    public static final String FREE_MEMORY = "freeMemory";
    public static final String JOB_SIZE = "jobSize";
    public static final String AVERAGE_FILE_SIZE = "avgFileSize";
    public static final String TOTAL_BYTES_SENT = "totalBytesSent";
    public static final String COMPRESSION = "compression";
    public static final String ALLOCATED_MEMORY = "allocatedMemory";
    public static final String SOURCE_TYPE = "sourceType";
    public static final String DESTINATION_TYPE = "destType";
    public static final String READ_THROUGHPUT = "read_throughput";
    public static final String WRITE_THROUGHPUT = "write_throughput";

    public static final String SOURCE_CRED_ID = "sourceCredentialId";
    public static final String DESTINATION_CRED_IT = "destinationCredentialId";

}