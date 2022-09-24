package org.sanofi.eimadaptor.controller;

import lombok.extern.slf4j.Slf4j;
import org.sanofi.eimadaptor.service.ISubmitSparkJobService;
import org.sanofi.eimadaptor.utils.returnFormat.AirFlowR;
import org.sanofi.eimadaptor.vo.SparkAppParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/sparkJob")
public class SparkJobController {

    @Autowired
    private ISubmitSparkJobService submitSparkJobService;

    @PostMapping("/dqrule")
    @ResponseBody
    public AirFlowR dqRule(@RequestBody SparkAppParam sparkAppParam) throws IOException, InterruptedException {
        System.out.println(sparkAppParam);
        String s = submitSparkJobService.submitJob(sparkAppParam.getSparkAppParam());
        return AirFlowR.ok(s, "" + "执行成功");

    }
}
