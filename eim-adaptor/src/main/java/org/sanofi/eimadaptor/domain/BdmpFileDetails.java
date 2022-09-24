package org.sanofi.eimadaptor.domain;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description BDMP文件相应信息
 * @date 2022/9/24 22:06:02
 */
@NoArgsConstructor
@Data
public class BdmpFileDetails {
    private Integer biId;
    private String sourceSystem;
    private Integer bifId;
    private String tabCommentCn;
    private String loadSolution;
    private String fullDelta;
    private String ifPhysicalDeletion;
    private String loadKey;
    private String fileType;
    private String delimiter;
    private String quote;
    private String ddlName;



    public String getTabCommentCn() {
        if ("".equals(tabCommentCn) || tabCommentCn == null) {
            return "no table comment";
        }
        return tabCommentCn;
    }


    public String getLoadKey() {
        if ("".equals(loadKey) || loadKey == null) {
            return "";
        }
        return loadKey;
    }

    public String getFileType() {
        if ("".equals(fileType) || fileType == null) {
            return "CSV";
        }
        return fileType;
    }

    public String getDelimiter() {
        if ("".equals(delimiter) || delimiter == null) {
            return ",";
        }
        return delimiter;
    }

    public String getQuote() {
        if ("".equals(quote) || quote == null || quote.equals("\"")) {
            return "\\\"";
        }
        return quote;
    }

}
