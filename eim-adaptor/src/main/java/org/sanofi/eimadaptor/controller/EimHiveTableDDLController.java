package org.sanofi.eimadaptor.controller;

import org.sanofi.eimadaptor.service.IHiveTableDdlService;
import org.sanofi.eimadaptor.utils.returnFormat.AirFlowR;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.sanofi.eimadaptor.utils.date.DateUtils;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description hive表DDL语句 ldg stg rej ods
 * @date 2022/9/24 22:32:36
 */
@RestController
@RequestMapping("eim/ddl")
public class EimHiveTableDDLController {

    @Autowired
    IHiveTableDdlService IHiveTableDdlService;

    @GetMapping("/generate/{fileName}")
    public AirFlowR generate(@PathVariable String fileName) throws IOException {

        Integer tablesNum = IHiveTableDdlService.validTablesNum(fileName);
        if (tablesNum == 0) {
            return AirFlowR.failed(fileName + "文件在bdmp中无法找到，确定配置以及条件是否正确");
        } else if (tablesNum > 1) {
            return AirFlowR.failed(fileName + "文件在bdmp中存在多条记录,修改bdmp数据，确保仅有一条有效数据");
        } else {
            if (IHiveTableDdlService.generateTableDdl(fileName)) {
                return AirFlowR.ok(fileName + "文件写入成功");

            } else {
                return AirFlowR.failed(fileName + "文件写入失败");
            }

        }
    }

    @PostMapping("/generate")
    @ResponseBody
    public AirFlowR generate(@RequestBody Map<String, Object> map) throws IOException {

        List<String> fileNameList = (List<String>) map.get("fileName");
        String jobDate =(String) map.get("jobDate");

        for (String f : fileNameList) {
            Integer tablesNum = IHiveTableDdlService.validTablesNum(f);
            if (tablesNum == 0) {
                return AirFlowR.failed(f + "文件在bdmp中无法找到，确定配置以及条件是否正确");
            } else if (tablesNum > 1) {
                return AirFlowR.failed(f + "文件在bdmp中存在多条记录,修改bdmp数据，确保仅有一条有效数据");
            } else if (!IHiveTableDdlService.generateTableDdl(f,jobDate)) {
                return AirFlowR.failed(f + "文件写入失败");
            }
        }
        return AirFlowR.ok("successful", fileNameList + "执行成功");

    }
}
