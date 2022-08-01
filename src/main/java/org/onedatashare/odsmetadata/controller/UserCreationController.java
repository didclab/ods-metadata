package org.onedatashare.odsmetadata.controller;

import org.onedatashare.odsmetadata.services.CertService;
import org.onedatashare.odsmetadata.services.UserCreationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Nonnull;
import java.util.regex.Pattern;

/**
 * This controller allows to create users in CockroachDB
 */
@RestController
@RequestMapping(value="/userController", produces = MediaType.APPLICATION_JSON_VALUE)
public class UserCreationController {

    private static final Logger logger = LoggerFactory.getLogger(UserCreationController.class);
    private static final String REGEX_PATTERN = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
            + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$";


    @Autowired
    UserCreationService userCreationService;

    @Autowired
    CertService certService;
    /**
     * CreateUser()
     */
    @PostMapping("/user/create")
    public boolean createUser(@RequestParam(value="username", required = true) String username,
                              @RequestParam(value="password", required = true) String password)
            throws Exception {
        boolean flag = false;
        if(validateuserId(username)){
            String name = splitUserId(username);
            flag = userCreationService.createUser(name, password);
        }
        if(flag){
            certService.createCertificate();
        }
        return flag;
    }

    /**
     * DeleteUser()
     */
    @PostMapping("/user/delete")
    public boolean deleteUser(@RequestParam(value="username", required = true) String username)
            throws Exception {
        boolean flag = false;
        if(validateuserId(username)){
            flag= userCreationService.deleteUserService(username);
        }
        if(flag){
            certService.createCertificate();
        }
        return flag;
    }

    /**
     * UpdateUser()
     */
    @PostMapping("/user/update")
    public boolean updateUser(@RequestParam(value="username", required = true) @Nonnull String username,
                              @RequestParam(value="password", required = true) @Nonnull String password){

        validateuserId(username);
        return true;
    }

    /**
     * RefreshUser()
     */
    @PostMapping("/user/refresh")
    public boolean refreshUser(@RequestParam(value="username", required = true) @Nonnull String username,
                               @RequestParam(value="password", required = true) @Nonnull String password){
        validateuserId(username);
        return true;
    }

    private boolean validateuserId(String userId){
        return Pattern.compile(REGEX_PATTERN)
                .matcher(userId)
                .matches();
    }

    private String splitUserId(String userid){
        return userid.substring(0,userid.indexOf("@"));



    }

    @GetMapping("/user/ssl")
    public boolean genSsl() throws Exception {
        boolean flag = false;
        certService.createCertificate();
        return flag;
    }


}
