package org.onedatashare.odsmetadata.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

@Service
public class CertService {


    private static final Logger logger = LoggerFactory.getLogger(CertService.class);


    public void certificateGen(){
        boolean isWindows = System.getProperty("os.name")
                .toLowerCase().startsWith("windows");
        logger.info("line 21: is it windows? "+isWindows);
        try {
        ProcessBuilder builder = new ProcessBuilder();
        File resourceFile = new File("../myFile.txt");


            if (isWindows) {
            builder.command("cmd.exe", "/c", "dir");
        } else {
            builder.command("sh", "-c", "mkdir userCerts userDirectory ; " +
                    "openssl genrsa -out userDirectory/ca.key 2048 ; chmod 400 userDirectory/ca.key ; " +
                    "openssl req -new -x509 -config ca.cnf -key userDirectory/ca.key -out userCerts/ca.crt " +
                    "-days 365 -batch ; rm -f index.txt serial.txt ; touch index.txt ; echo '01' > serial.txt ; " +
                    "openssl genrsa -out userCerts/client.ods.key 2048 ; chmod 400 userCerts/client.ods.key ; " +
                    "openssl req -new -config client.cnf -key userCerts/client.ods.key -out client.ods.csr -batch ; " +
                    "openssl ca -config ca.cnf -keyfile userDirectory/ca.key -cert userCerts/ca.crt -policy " +
                    "signing_policy -extensions signing_client_req -out userCerts/client.ods.crt -outdir userCerts/ -in " +
                    "client.ods.csr -batch");

        }

        builder.directory(new File(System.getProperty("user.home")));
        Process process = builder.start();

        StreamGobbler streamGobbler =
                new StreamGobbler(process.getInputStream(), System.out::println);

        Executors.newSingleThreadExecutor().submit(streamGobbler);

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static class StreamGobbler implements Runnable {
        private InputStream inputStream;
        private Consumer<String> consumer;

        public StreamGobbler(InputStream inputStream, Consumer<String> consumer) {
            this.inputStream = inputStream;
            this.consumer = consumer;
        }

        @Override
        public void run() {
            new BufferedReader(new InputStreamReader(inputStream)).lines()
                    .forEach(consumer);
        }
    }

}
