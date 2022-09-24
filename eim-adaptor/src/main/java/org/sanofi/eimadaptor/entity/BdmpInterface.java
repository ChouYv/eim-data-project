package org.sanofi.eimadaptor.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

@Data
@TableName("app_upt.bdmp_interface")
public class BdmpInterface {
    private long id;
    private String name;
    private String createdUser;
    private String lastUser;
    private long status;
    private String cosBucket;
    private String cosKey;
    private String deleted;
    private String annotation;
    private long rawId;
    private java.sql.Timestamp insertTime;
    private java.sql.Timestamp updateTime;
    private String sourceSystem;
    private String sourceSystemHosting;
    private String systemOwnerEmail;
    private String interfaceCategory;
    private String sourceSystemCategory;
    private String sourceSystemHostingType;
    private String pretreatmentType;

}
