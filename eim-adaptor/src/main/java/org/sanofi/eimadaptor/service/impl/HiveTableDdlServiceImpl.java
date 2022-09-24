package org.sanofi.eimadaptor.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.sanofi.eimadaptor.domain.BdmpFileDetails;
import org.sanofi.eimadaptor.domain.BdmpFileFieldDetails;
import org.sanofi.eimadaptor.domain.mapper.BdmpFileDetailsMapper;
import org.sanofi.eimadaptor.domain.mapper.BdmpFileFieldDetailsMapper;
import org.sanofi.eimadaptor.service.IHiveTableDdlService;
import org.sanofi.eimadaptor.utils.date.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;


/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description IHiveTableDdlService实现
 * @date 2022/9/25 00:03:18
 */
@Service
@Slf4j
public class HiveTableDdlServiceImpl implements IHiveTableDdlService {

    @Autowired
    BdmpFileDetailsMapper BdmpFileDetailsMapper;

    @Autowired
    BdmpFileFieldDetailsMapper BdmpFileFieldDetailsMapper;


    @Value("${hive.table.location}")
    private String cosPath;
    @Value("${hive.table.ddl-file-path}")
    private String ddlWritePer;

    //手动维护 数据库类型映射关系
    private Map<String, String> filedMap = new HashMap<String, String>() {{
        put("numeric(18,4)", "Decimal(18,4)");
        put("decimal", "Decimal");
        put("varchar", "String");
        put("NVARCHAR2", "String");
        put("int", "Int");
        put("CHAR", "String");
        put("nvarchar", "String");
        put("TIMESTAMP", "Timestamp");
        put("bool", "Boolean");
        put("timestamp", "Timestamp");
        put("text", "String");
        put("DATE", "Date");
        put("time", "String");
        put("Varchar2", "String");
        put("bigint", "Bigint");
        put("numeric", "Decimal");
        put("date", "Date");
        put("VARCHAR2", "String");
        put("NUMBER", "String");
    }};

    //手动维护CSV文件类型种类
    private ArrayList<String> csvTypes = new ArrayList<>(Arrays.asList("Table", "CSV", "EXCEL"));


    @Override
    public Boolean fieldTableExist(String fileName) {
        Integer bifId = BdmpFileDetailsMapper
                .getBdmpFileDetailsByFileAliasOrByName(fileName)
                .get(0)
                .getBifId();
        return BdmpFileFieldDetailsMapper.selectByFileId(bifId).size() != 0;
    }


    @Override
    public Integer validTablesNum(String fileName) {
        return BdmpFileDetailsMapper.getBdmpFileDetailsByFileAliasOrByName(fileName).size();
    }

    @Override
    public List<String> getTablePk(String fileName) {
        BdmpFileDetails bdmpFileDetails = getBdmpFileDetails(fileName);

        List<BdmpFileFieldDetails> bdmpFileFieldDetailsList
                = getBdmpFileFieldDetailsList(bdmpFileDetails.getBifId());

        Integer size = (int) bdmpFileFieldDetailsList.stream().filter(
                p -> "Y".equals(p.getPrimaryKey())
        ).count();

        if (size == 0) {
            return bdmpFileFieldDetailsList.stream().filter(
                    p -> "Y".equals(p.getBusinessKey())
            ).map(
                    d -> {
                        StringBuilder sbPk = new StringBuilder();
                        sbPk.append(d.getPName());
                        return sbPk.toString();
                    }
            ).collect(Collectors.toList());
        } else {
            return bdmpFileFieldDetailsList.stream().filter(
                    p -> "Y".equals(p.getPrimaryKey())
            ).map(
                    d -> {
                        StringBuilder sbPk = new StringBuilder();
                        sbPk.append(d.getPName());
                        return sbPk.toString();
                    }
            ).collect(Collectors.toList());
        }

    }

    @Override
    public Boolean generateTableDdl(String fileName, String jobDate) throws IOException {

        Boolean flag = true;
        File file = new File(ddlWritePer + jobDate + "/" + fileName + ".sql");

        if (!file.getParentFile().exists()) {
            file.getParentFile().mkdirs();
        }
        if (!file.exists()) {
            file.createNewFile();
        }
        PrintWriter printWriter = new PrintWriter(file);


        String ssss = generateLdgTableDdl(fileName) + "\n\n" +
                generateStgTableDdl(fileName) + "\n\n" +
                generateRejTableDdl(fileName) + "\n\n" +
                generateOdsTableDdl(fileName) + "\n\n" +
                generateLdgPkTableDdl(fileName) + "\n\n" +
                generateStgPkTableDdl(fileName) + "\n\n" +
                generateRejPkTableDdl(fileName);
        try {
            printWriter.write(ssss);
        } catch (Exception e) {
            flag = false;
            log.warn(e.getMessage());
        } finally {
            printWriter.close();
        }

        return flag;
    }

    @Override
    public Boolean generateTableDdl(String fileName) throws IOException {
        String jobDate = Integer.toString(DateUtils.getDateNo(new Date()));
        return generateTableDdl(fileName, jobDate);
    }


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

    private String generateLdgTableDdl(String fileName) {

        BdmpFileDetails bdmpFileDetails = getBdmpFileDetails(fileName);

        List<BdmpFileFieldDetails> bdmpFileFieldDetailsList
                = getBdmpFileFieldDetailsList(bdmpFileDetails.getBifId());


        //获取ldg层 库表
        String ldgTableName = getHiveTableName("ldg", bdmpFileDetails.getDdlName());
        //获取文件类型
        String fileType = bdmpFileDetails.getFileType();
        //获取字段 数据类型
        StringBuilder sb = new StringBuilder();
        for (BdmpFileFieldDetails i : bdmpFileFieldDetailsList) {
            sb.append("`").append(i.getPName()).append("` string \n,");
        }

        String preSql = "drop table if exists " + ldgTableName + ";\n" +
                "CREATE EXTERNAL TABLE IF NOT EXISTS " + ldgTableName + "(\n";

        String midSql = sb.substring(0, sb.length() - 1);

        String sufSql;

        if (csvTypes.contains(fileType)) {
            sufSql = ") COMMENT '" + bdmpFileDetails.getTabCommentCn() + "' \n " +
                    "PARTITIONED BY (`eim_dt` string) \n" +
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" +
                    "with serdeproperties(\n" +
                    "'separatorChar' = '" + bdmpFileDetails.getDelimiter() + "',\n" +
                    "'quoteChar' = '" + bdmpFileDetails.getQuote() + "')\n" +
                    "LOCATION '" + cosPath + "/" + bdmpFileDetails.getSourceSystem() + "/" + bdmpFileDetails.getDdlName() + "'\n" +
                    "TBLPROPERTIES ('skip.header.line.count'='1');";
        } else {
            sufSql = ") \nCOMMENT '" + bdmpFileDetails.getTabCommentCn() + "' \nPARTITIONED BY (`eim_dt` string); \n";
        }

        String ldgTableDdlSql = preSql + midSql + sufSql;

        return ldgTableDdlSql;

    }

    private String generateStgTableDdl(String fileName) {

        BdmpFileDetails bdmpFileDetails = getBdmpFileDetails(fileName);

        List<BdmpFileFieldDetails> bdmpFileFieldDetailsList
                = getBdmpFileFieldDetailsList(bdmpFileDetails.getBifId());
        //获取stg层 库表
        String tableName = getHiveTableName("stg", bdmpFileDetails.getDdlName());
        //获取文件类型
        String fileType = bdmpFileDetails.getFileType();
        //获取字段 数据类型
        StringBuilder sb = new StringBuilder();
        for (BdmpFileFieldDetails i : bdmpFileFieldDetailsList) {
            sb.append("`")
                    .append(i.getPName())
                    .append("` string comment '")
                    .append(i.getFieldsCommentCn())
                    .append(" || ")
                    .append(i.getFieldsCommentEn())
                    .append("' \n,");
        }
        String preSql = "drop table if exists " + tableName + ";\n" +
                "CREATE TABLE IF NOT EXISTS " + tableName + "(\n";

        String midSql = sb.substring(0, sb.length() - 1);

        String sufSql = ") \nCOMMENT '" + bdmpFileDetails.getTabCommentCn() + "' \nPARTITIONED BY (`eim_dt` string) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY '\\001' \n" +
                "LINES TERMINATED BY '\\n' \n" +
                "stored as orc;";

        String stgTableDdlSql = preSql + midSql + sufSql;

        return stgTableDdlSql;
    }

    private String generateRejTableDdl(String fileName) {

        BdmpFileDetails bdmpFileDetails = getBdmpFileDetails(fileName);

        List<BdmpFileFieldDetails> bdmpFileFieldDetailsList
                = getBdmpFileFieldDetailsList(bdmpFileDetails.getBifId());
        //获取stg层 库表
        String tableName = getHiveTableName("rej", bdmpFileDetails.getDdlName());
        //获取文件类型
        String fileType = bdmpFileDetails.getFileType();
        //获取字段 数据类型
        StringBuilder sb = new StringBuilder();
        for (BdmpFileFieldDetails i : bdmpFileFieldDetailsList) {
            sb.append("`")
                    .append(i.getPName())
                    .append("` string comment '")
                    .append(i.getFieldsCommentCn())
                    .append(" || ")
                    .append(i.getFieldsCommentEn())
                    .append("' \n,");
        }
        String preSql = "drop table if exists " + tableName + ";\n" +
                "CREATE TABLE IF NOT EXISTS " + tableName + "(\n";

        String midSql = sb.substring(0, sb.length() - 1) + ",`eim_flag` string ";

        String sufSql = ") \nCOMMENT '" + bdmpFileDetails.getTabCommentCn() + "' \nPARTITIONED BY (`eim_dt` string) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY '\\001' \n" +
                "LINES TERMINATED BY '\\n' \n" +
                "stored as orc;";

        String rejTableDdlSql = preSql + midSql + sufSql;

        return rejTableDdlSql;
    }

    private String generateOdsTableDdl(String fileName) {

        BdmpFileDetails bdmpFileDetails = getBdmpFileDetails(fileName);

        List<BdmpFileFieldDetails> bdmpFileFieldDetailsList
                = getBdmpFileFieldDetailsList(bdmpFileDetails.getBifId());
        //获取stg层 库表
        String tableName = getHiveTableName("ods", bdmpFileDetails.getDdlName());
        //获取文件类型
        String fileType = bdmpFileDetails.getFileType();

        //ods分区方式
        String partitionWay = "";
        if ("full delete full load by key".equals(bdmpFileDetails.getLoadSolution())) {
            partitionWay = "loadKey";
        } else if (!"full delete full load by key".equals(bdmpFileDetails.getLoadSolution())
                && "Full".equals(bdmpFileDetails.getFullDelta())) {
            partitionWay = "";
        } else if (!"full delete full load by key".equals(bdmpFileDetails.getLoadSolution())
                && "Delta".equals(bdmpFileDetails.getFullDelta())) {
            partitionWay = "eim_dt";
        }

        //分区字段
        List<String> loadKeyList = Arrays.asList(bdmpFileDetails.getLoadKey().split(","));


        //获取字段 数据类型
        StringBuilder sb = new StringBuilder();
        for (BdmpFileFieldDetails i : bdmpFileFieldDetailsList) {
            //以loadkey方式分区，并且字段含有loadkey的 排除掉
            if ("loadKey".equals(partitionWay) && loadKeyList.contains(i.getPName())) {
                continue;
            }
            String hiveType = "String";
            String s = filedMap.get(i.getFieldType());

            if (s != null) {
                hiveType = s;
            }

            sb.append("`")
                    .append(i.getPName())
                    .append("` " + hiveType + " comment '")
                    .append(i.getFieldsCommentCn())
                    .append(" || ")
                    .append(i.getFieldsCommentEn())
                    .append("' \n,");

        }
        String preSql = "drop table if exists " + tableName + ";\n" +
                "CREATE TABLE IF NOT EXISTS " + tableName + "(\n";

        String midSql = sb.substring(0, sb.length() - 1) + ",`etl_is_hard_del` int COMMENT '0,1'\n," +
                "`etl_created_ts` timestamp COMMENT 'eim_创建时间'\n," +
                "`etl_modified_ts` timestamp COMMENT 'eim_修改时间'";

        if (!"eim_dt".equals(partitionWay)) {
            midSql = midSql + "\n,`eim_dt` string";
        }

        String sufSqlOne = ") \nCOMMENT '" + bdmpFileDetails.getTabCommentCn() + "' ";

        String sufSqlTwo;

        //根据分区方式选择相应分区键
        if ("loadKey".equals(partitionWay)) {
            sufSqlTwo = "PARTITIONED BY (" + loadKeyList.stream().map(k -> "`" + k + "` string").collect(Collectors.joining(",")) + ")\n";
        } else if ("eim_dt".equals(partitionWay)) {
            sufSqlTwo = "PARTITIONED BY (`eim_dt` string) \n";
        } else {
            sufSqlTwo = "";
        }

        String sufSqlThree = "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY '\\001' \n" +
                "LINES TERMINATED BY '\\n' \n" +
                "stored as orc;";
        String sufSql = sufSqlOne + sufSqlTwo + sufSqlThree;

        String odsTableDdlSql = preSql + midSql + sufSql;

        return odsTableDdlSql;
    }


    private String generateLdgPkTableDdl(String fileName) {


        BdmpFileDetails bdmpFileDetails = getBdmpFileDetails(fileName);

        List<BdmpFileFieldDetails> bdmpFileFieldDetailsList
                = getBdmpFileFieldDetailsList(bdmpFileDetails.getBifId());


        //获取ldg层 库表
        String ldgTableName = getHiveTableName("ldgPk", bdmpFileDetails.getDdlName());
        //获取文件类型
        String fileType = bdmpFileDetails.getFileType();


        String preSql = "drop table if exists " + ldgTableName + ";\n" +
                "CREATE EXTERNAL TABLE IF NOT EXISTS " + ldgTableName + "(\n";

        String midSql = getTablePk(fileName).stream().map(p -> "`" + p + "` string").collect(Collectors.joining(","));

        String sufSql;

        if (csvTypes.contains(fileType)) {
            sufSql = ") COMMENT '" + bdmpFileDetails.getTabCommentCn() + "' \n " +
                    "PARTITIONED BY (`eim_dt` string) \n" +
                    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" +
                    "with serdeproperties(\n" +
                    "'separatorChar' = '" + bdmpFileDetails.getDelimiter() + "',\n" +
                    "'quoteChar' = '" + bdmpFileDetails.getQuote() + "')\n" +
                    "LOCATION '" + cosPath + "/" + bdmpFileDetails.getSourceSystem() + "/" + bdmpFileDetails.getDdlName() + "_pk'\n" +
                    "TBLPROPERTIES ('skip.header.line.count'='1');";
        } else {
            sufSql = ") \nCOMMENT '" + bdmpFileDetails.getTabCommentCn() + "' \nPARTITIONED BY (`eim_dt` string); \n";
        }

        String ldgPkTableDdlSql = preSql + midSql + sufSql;

        return ldgPkTableDdlSql;

    }

    private String generateStgPkTableDdl(String fileName) {


        BdmpFileDetails bdmpFileDetails = getBdmpFileDetails(fileName);

        //获取ldg层 库表
        String ldgTableName = getHiveTableName("stgPk", bdmpFileDetails.getDdlName());


        String preSql = "drop table if exists " + ldgTableName + ";\n" +
                "CREATE EXTERNAL TABLE IF NOT EXISTS " + ldgTableName + "(\n";

        String midSql = getTablePk(fileName).stream().map(p -> "`" + p + "` string").collect(Collectors.joining(","));

        String sufSql = ") \nCOMMENT '" + bdmpFileDetails.getTabCommentCn() + "' \nPARTITIONED BY (`eim_dt` string) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY '\\001' \n" +
                "LINES TERMINATED BY '\\n' \n" +
                "stored as orc;";
        String stgPkTableDdlSql = preSql + midSql + sufSql;

        return stgPkTableDdlSql;

    }

    private String generateRejPkTableDdl(String fileName) {


        BdmpFileDetails bdmpFileDetails = getBdmpFileDetails(fileName);

        //获取ldg层 库表
        String ldgTableName = getHiveTableName("rejPk", bdmpFileDetails.getDdlName());

        String preSql = "drop table if exists " + ldgTableName + ";\n" +
                "CREATE EXTERNAL TABLE IF NOT EXISTS " + ldgTableName + "(\n";

        String midSql = getTablePk(fileName).stream().map(p -> "`" + p + "` string").collect(Collectors.joining(",")) + "\n,`eim_flag` string ";

        String sufSql = ") \nCOMMENT '" + bdmpFileDetails.getTabCommentCn() + "' \nPARTITIONED BY (`eim_dt` string) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY '\\001' \n" +
                "LINES TERMINATED BY '\\n' \n" +
                "stored as orc;";
        String rejPkTableDdlSql = preSql + midSql + sufSql;

        return rejPkTableDdlSql;

    }

}
