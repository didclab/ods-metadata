package org.onedatashare.odsmetadata.controller;

import org.onedatashare.odsmetadata.model.OdsUser;
import org.onedatashare.odsmetadata.services.InfluxIOService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
public class DatabaseUserController {

    @Autowired
    InfluxIOService influxIOService;

    @PostMapping
    public void createDatabaseUser(@RequestBody OdsUser user){
        influxIOService.onboardOdsUser(user.getUserName(), user.getPassword());
    }
}
