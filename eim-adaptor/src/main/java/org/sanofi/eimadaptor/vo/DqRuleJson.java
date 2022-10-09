package org.sanofi.eimadaptor.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DqRuleJson {
    private String jobDate;
    private String ldgTableName;
    private String stgTableName;
    private String rejTableName;
    private String ldgPkTableName;
    private String stgPkTableName;
    private String rejPkTableName;
    private List<String> priKeyArr;
    private Map<String, Map<String, String>> RuleCol;
}
