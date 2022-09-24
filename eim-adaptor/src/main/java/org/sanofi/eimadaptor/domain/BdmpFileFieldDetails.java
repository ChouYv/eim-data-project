package org.sanofi.eimadaptor.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description 获取文件对应表字段信息
 * @date 2022/9/24 22:49:55
 */
@NoArgsConstructor
@Data
public class BdmpFileFieldDetails {
    private String pName;
    private String name;
    private String fieldAlias;
    private String fieldType;
    private String valueRange;
    private Integer index;
    private String primaryKey;
    private String businessKey;
    private String fieldsCommentEn;
    private String fieldsCommentCn;
    private String ifEnumField;
    private String ifNotNull;
    private String timestampFormat;
}
