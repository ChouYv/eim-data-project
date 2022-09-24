package org.sanofi.eimadaptor.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("app_upt.bdmp_interface_file")
public class BdmpInterfaceFile {
    private long id;
    private long interfaceId;
    private String name;
    private String fileType;
    private String delimiter;
    private String quote;
    private String encoding;
    private String fullDelta;
    private long status;
    private java.sql.Timestamp insertTime;
    private java.sql.Timestamp updateTime;
    private String tabCommentCn;
    private String gbu;
    private String comments;
    private String subjectDomain1StLevel;
    private String fileAlias;
    private String tabCommentEn;
    private String dataType;
    private String availableDay;
    private String ifPhysicalDeletion;
    private String physicalDeletionFrequency;
    private String source;
    private String importantDataIndicator;
    private String frequency;
    private String availableTime;
    private String initialDataFile;
    private String loadSolution;
    private String entity;
    private String subjectDomain2NdLevel;
    private String avaliableMonth;
    private String contactsEmail;
    private String loadKey;
    private String runPlatform;
    private String ifKeepInInboundCos;
    private String fileRegex;
    private String fileNameRegex;
    private String fileCategory;
    private String pretreatmentType;
    private String incrementKey;

    public String getGbu() {
        if("".equals(gbu)||gbu==null){
            //去除该属性的前后空格并进行非空非null判断
            return "默认值~~~~";
        }
        return gbu;
    }


}
