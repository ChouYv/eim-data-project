package org.sanofi.eimadaptor.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("app_upt.bdmp_interface_field")
public class BdmpInterfaceField {
    private long id;
    private long fileId;
    private String name;
    private String fieldType;
    private long fieldLength;
    private long fieldPrecision;
    private String valueRange;
    private long status;
    private long index;
    private java.sql.Timestamp insertTime;
    private java.sql.Timestamp updateTime;
    private String primaryKey;
    private String businessKey;
    private String dataFileName;
    private String dataQualityRuleType1;
    private String dataQualityRuleType2;
    private String dataQualityRuleDescription2;
    private String dataQualityRuleType3;
    private String fkRelation;
    private String sample;
    private String dataQualityRuleDescription3;
    private String fieldsCommentEn;
    private String fieldsCommentCn;
    private String upstreamSystem;
    private String roleOfDataOwner;
    private String comment;
    private String businessDefinition;
    private String piplIndicator;
    private String ifEnumField;
    private String enumDescription;
    private String dataOwner;
    private String departmentOfDataOwner;
    private String dataQualityRuleDescription1;
    private String ifSystemField;
    private String fieldAlias;
    private String ifNotNull;
    private String A;
    private String timestampFormat;
}
