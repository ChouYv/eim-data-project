package org.sanofi.eimadaptor.service.impl;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.sanofi.eimadaptor.domain.BdmpFileDetails;
import org.sanofi.eimadaptor.domain.BdmpFileFieldDetails;
import org.sanofi.eimadaptor.domain.mapper.BdmpFileDetailsMapper;
import org.sanofi.eimadaptor.domain.mapper.BdmpFileFieldDetailsMapper;
import org.sanofi.eimadaptor.service.IDqRuleService;
import org.sanofi.eimadaptor.vo.DqRuleJson;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class DqRuleServiceImpl implements IDqRuleService {

    @Autowired
    org.sanofi.eimadaptor.domain.mapper.BdmpFileDetailsMapper BdmpFileDetailsMapper;

    @Autowired
    org.sanofi.eimadaptor.domain.mapper.BdmpFileFieldDetailsMapper BdmpFileFieldDetailsMapper;

    private List<String> priKeyColList;

    private Map<String, Map<String, String>> filedDetailsMap = new HashMap<>();


    private BdmpFileDetails getBdmpFileDetails(String fileName) {
        return BdmpFileDetailsMapper.getBdmpFileDetailsByFileAliasOrByName(fileName).get(0);
    }


    private List<BdmpFileFieldDetails> getBdmpFileFieldDetailsList(Integer fileId) {
        return BdmpFileFieldDetailsMapper.selectByFileId(fileId);
    }

    private String getHiveTableName(String stg, String ddlName) {
        switch (stg) {
            case "ldg":
                return "ldg." + ddlName;
            case "stg":
                return "stg." + ddlName;
            case "rej":
                return "rej.ldg_" + ddlName + "_rej";
            case "ldgPk":
                return "ldg." + ddlName + "_pk";
            case "stgPk":
                return "stg." + ddlName + "_pk";
            case "rejPk":
                return "rej.ldg_" + ddlName + "_pk";
            case "ods":
                return "ods." + ddlName;
            default:
                return "";
        }

    }

    @Override
    public DqRuleJson getDqRuleJson(String fileName) {
        DqRuleJson dqRuleJson = new DqRuleJson();
        log.info(dqRuleJson.toString());
        BdmpFileDetails bdmpFileDetails = getBdmpFileDetails(fileName);

        List<BdmpFileFieldDetails> bdmpFileFieldDetailsList
                = getBdmpFileFieldDetailsList(bdmpFileDetails.getBifId());

        //      表名获取
        if ("Y".equals(bdmpFileDetails.getIfPhysicalDeletion())) {
            dqRuleJson.setLdgPkTableName(getHiveTableName("ldgPk", bdmpFileDetails.getDdlName()));
            dqRuleJson.setStgPkTableName(getHiveTableName("stgPk", bdmpFileDetails.getDdlName()));
            dqRuleJson.setRejPkTableName(getHiveTableName("rejPk", bdmpFileDetails.getDdlName()));
        }
        dqRuleJson.setLdgTableName(getHiveTableName("ldg", bdmpFileDetails.getDdlName()));
        dqRuleJson.setStgTableName(getHiveTableName("stg", bdmpFileDetails.getDdlName()));
        dqRuleJson.setRejTableName(getHiveTableName("rej", bdmpFileDetails.getDdlName()));

        //     主键获取
        List<String> priKeyList = bdmpFileFieldDetailsList.stream().map(BdmpFileFieldDetails::getPrimaryKey).collect(Collectors.toList());
        if (priKeyList.contains("Y")) {
            priKeyColList = bdmpFileFieldDetailsList
                    .stream()
                    .filter(x -> "Y".equals(x.getPrimaryKey()))
                    .map(BdmpFileFieldDetails::getPName)
                    .collect(Collectors.toList());
        } else {
            priKeyColList = bdmpFileFieldDetailsList
                    .stream()
                    .filter(x -> "Y".equals(x.getBusinessKey()))
                    .map(BdmpFileFieldDetails::getPName)
                    .collect(Collectors.toList());
        }
        dqRuleJson.setPriKeyArr(priKeyColList);

        //     字段清洗规则获取
        for (BdmpFileFieldDetails x : bdmpFileFieldDetailsList) {
            //   TODO         哪些类型是需要进行时间校验的    大写的DATE 会带时间格式
            List<String> dateTypeList = Arrays.asList("TIMESTAMP", "timestamp", "DATE", "date", "time");

//            整型校验
            List<String> intTypeList = Arrays.asList("int", "bigint");


            Map<String, String> tmpMap = new HashMap<>();
            tmpMap.put("nullCheck", "Y".equals(x.getIfNotNull()) ? "Y" : "N");
            tmpMap.put("dateFormatted", null == x.getTimestampFormat() || 0 == x.getTimestampFormat().length() ? "yyyy-MM-dd hh:mm:ss" : x.getTimestampFormat());
//            TODO 强转一下带： - 空格 的Str
//            if (null == x.getTimestampFormat() || 0 == x.getTimestampFormat().length()) {
//                tmpMap.put("dateFormatted", "yyyyf1MMf1ddf2hhf3mmf3ss");
//            } else {
//                tmpMap.put("dateFormatted", x.getTimestampFormat().replaceAll("-", "f1").replaceAll(" ", "f2").replaceAll(":", "f3"));
//            }
            tmpMap.put("dateCheck", dateTypeList.contains(x.getFieldType()) ? "Y" : "N");
            tmpMap.put("intCheck", intTypeList.contains(x.getFieldType()) ? "Y" : "N");
            tmpMap.put("enumRange", x.getValueRange());
            tmpMap.put("enumCheck", "Y".equals(x.getIfEnumField()) ? "Y" : "N");
            tmpMap.put("uniqueCheck", "Y".equals(x.getPrimaryKey()) || "Y".equals(x.getBusinessKey()) ? "Y" : "N");
            filedDetailsMap.put(x.getPName(), tmpMap);
        }
        dqRuleJson.setRuleCol(filedDetailsMap);

        return dqRuleJson;
    }
}
