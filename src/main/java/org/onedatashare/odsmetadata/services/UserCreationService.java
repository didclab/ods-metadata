package org.onedatashare.odsmetadata.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import javax.validation.constraints.NotNull;
import java.time.LocalDate;

@Service
public class UserCreationService {

    private static final Logger logger = LoggerFactory.getLogger(UserCreationService.class);

    @Value("${account.valid.until}")
    private int VAL_UNTIL;

    private final String QUERY_CREATEUSER="create user ? WITH LOGIN PASSWORD ? VALID UNTIL ?";
    private final String QUERY_DELETEUSER="update pg_roles set rolcanlogin = false where rolname ?";
    private final String QUERY_UPDATEUSER=null;
    private final String QUERY_REFRESHUSER=null;
    private final String QUERY_ALLOWGRANT=null;
    private final String QUERY_REVOKEGRANT=null;
    private final String QUERY_UPDATEGRANT=null;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * @param dataSource
     */
    @Autowired
    public void setDataSource(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }


    LocalDate today =  LocalDate.now(); //pass valuntil date as today.plusDays(90);

    public int createUser(@NotNull String username, @NotNull String password){
       return jdbcTemplate.update(QUERY_CREATEUSER, username,password,today.plusDays(VAL_UNTIL));
    }

    public boolean deleteUserService(@NotNull String username){
        jdbcTemplate.update(QUERY_DELETEUSER,username);
        return true;
    }

    public boolean updateUserService(String username,String password){

        return true;
    }

    public boolean refreshUserService(String username,String password){

        return true;
    }


}
