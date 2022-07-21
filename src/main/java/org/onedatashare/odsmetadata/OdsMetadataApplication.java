package org.onedatashare.odsmetadata;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

@EnableDiscoveryClient
@SpringBootApplication
public class OdsMetadataApplication {

    public static void main(String[] args) {
        SpringApplication.run(OdsMetadataApplication.class, args);
    }

}
