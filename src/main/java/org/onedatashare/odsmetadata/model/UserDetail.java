package org.onedatashare.odsmetadata.model;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.sql.Timestamp;

@Data
@AllArgsConstructor
public class
UserDetail {
        private Long usesysid;
        private String usename;
        private boolean usecreatedb;
        private boolean usesuper;
        private boolean userepl;
        private boolean usebypassrls;
        private String passwd;
        private Timestamp valuntil;
        private String useconfig;
}
