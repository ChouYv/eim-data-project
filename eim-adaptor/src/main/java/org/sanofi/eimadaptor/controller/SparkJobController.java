package org.sanofi.eimadaptor.controller;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.sanofi.eimadaptor.service.IDqRuleService;
import org.sanofi.eimadaptor.service.ISubmitSparkJobService;
import org.sanofi.eimadaptor.utils.returnFormat.AirFlowR;
import org.sanofi.eimadaptor.vo.DqRuleJson;
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
    @Autowired
    private IDqRuleService dqRuleService;

    @PostMapping("/dqrule")
    @ResponseBody
    public AirFlowR dqRule(@RequestBody SparkAppParam sparkAppParam) throws IOException, InterruptedException {

        if (sparkAppParam.getExtraParams().get("fileName") == null) {
            return AirFlowR.failed("没有上传文件名   fileName");
        }
        if (sparkAppParam.getExtraParams().get("jobDate") == null) {
            return AirFlowR.failed("没有执行日期 yyyy-MM-dd  jobDate");
        }

        DqRuleJson sparkJobArgs = dqRuleService
                .getDqRuleJson(sparkAppParam.getExtraParams().get("fileName"));

        sparkJobArgs.setJobDate(sparkAppParam.getExtraParams().get("jobDate"));
        JSON.toJSONString(sparkJobArgs);
        
        /*
         * @Author yav
         * @Description //TODO replaceAll("}}","} }") 不可去掉，yarn模式 传递参数会屏蔽掉}}!!!!!!!!!!!
         * @Date  2022-10-08 04:30:30
         **/
        submitSparkJobService.submitJob(sparkAppParam.getSparkAppParam(),JSON.toJSONString(sparkJobArgs).replaceAll("}}","} }"));

        return AirFlowR.ok(JSON.toJSONString(sparkJobArgs), "" + "执行成功");

    }


}
