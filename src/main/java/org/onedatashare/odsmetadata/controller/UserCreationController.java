package org.onedatashare.odsmetadata.controller;

import org.onedatashare.odsmetadata.services.UserCreationService;
import org.onedatashare.odsmetadata.services.CertService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

/**
 * This controller allows to create users in CockroachDB
 * @author aishwaryarath
 */
@RestController
@RequestMapping(value="/user", produces = MediaType.APPLICATION_JSON_VALUE)
public class UserCreationController {

    private static final Logger logger = LoggerFactory.getLogger(UserCreationController.class);
    private static final String REGEX_PATTERN_EMAIL = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
            + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$";


    @Autowired
    UserCreationService userCreationService;

    @Autowired
    CertService certService;

    /**
     * Creates a new user in the database
     * @param username
     * @param password
     * @return
     */
    @PostMapping("/create")
    public ResponseEntity<String> createUser(@RequestParam(value="username", required = true) String username,
                                     @RequestParam(value="password", required = true) String password) {
        String name="";
        if(validateuserId(username)){
            name = splitUserId(username);
            try{
                int res  = userCreationService.createUser(name, password);
                logger.info("value of res: "+res);
                if(res==0) certService.certificateGen();

            } catch(DataAccessException ex) {
                logger.error("Exception occurred in user creation. ", ex);
                return new ResponseEntity<>(String.format("Exception occurred during user creation."),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            } catch (Exception e) {
                logger.error("Exception occurred in certificate creation. ", e);
                return new ResponseEntity<>(String.format("Exception occurred in certificate creation. "),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }

        }
        return new ResponseEntity<>(String.format("User: '%s' created successfully!", name),
                HttpStatus.CREATED);
    }

    /**
     * DeleteUser()
     */
    @PostMapping("/delete")
    public ResponseEntity<String> deleteUser(@RequestParam(value="username", required = true) String username) {
        String name="";
        if(validateuserId(username)) {
            name = splitUserId(username);
            try {
                userCreationService.deleteUserService(username);
            } catch (DataAccessException ex) {
                logger.error("Exception occurred in user deletion. ", ex);
                return new ResponseEntity<>(String.format("Exception occurred during user deletion."),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
        return new ResponseEntity<>(String.format("User: '%s' deleted successfully!", name),
                HttpStatus.ACCEPTED);
    }

    /**
     * UpdateUser()
     */
    @PostMapping("/update")
    public ResponseEntity<String> updateUser(@RequestParam(value="username", required = true) @Nonnull String username,
                              @RequestParam(value="password", required = true) @Nonnull String password){

        String name="";
        if(validateuserId(username)) {
            name = splitUserId(username);
            try {
                userCreationService.updateUserService(username,password);
            } catch (DataAccessException ex) {
                logger.error("Exception occurred in updating user's password. ", ex);
                return new ResponseEntity<>(String.format("Exception occurred in updating user's password."),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }

        }
        return new ResponseEntity<>(String.format("User: '%s' deleted successfully!", name),
                HttpStatus.ACCEPTED);
    }

    /**
     * RefreshUser() --> ask for date of certificate creation and compare it with localdate
     */
    @PostMapping("/refresh/cert")
    public ResponseEntity<String> refreshUser(@RequestParam(value="username", required = true) @Nonnull String username,
                               @RequestParam(value="password", required = true) @Nonnull String password) {
        String name="";
        if(validateuserId(username)) {
            name = splitUserId(username);
            try{
                certService.certificateGen();
            } catch (Exception e) {
                logger.error("Exception occurred in certificate creation. ", e);
                return new ResponseEntity<>(String.format("Exception occurred in certificate creation. "),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
        }
        return new ResponseEntity<>(String.format("Certificate generated for User: '%s'!", name),
                HttpStatus.CREATED);
    }

    private boolean validateuserId(String userId){
        return Pattern.compile(REGEX_PATTERN_EMAIL)
                .matcher(userId)
                .matches();
    }

    private String splitUserId(String userid){
        return userid.substring(0,userid.indexOf("@"));

    }

    @GetMapping("/user/ssl")
    public ResponseEntity<String> genSsl() throws Exception {
        boolean flag = false;
        certService.certificateGen();
        return new ResponseEntity<>(HttpStatus.CREATED);
    }


    @GetMapping("/user/cockroachdb")
    public ResponseEntity<String> cert() throws Exception {
        certService.certificateGen();
        return new ResponseEntity<>(HttpStatus.CREATED);
    }



}
