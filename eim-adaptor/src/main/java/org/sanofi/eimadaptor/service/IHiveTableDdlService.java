package org.sanofi.eimadaptor.service;

import org.sanofi.eimadaptor.utils.returnFormat.AirFlowR;

import java.io.IOException;
import java.util.List;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description hive表DDL语句初始化
 * @date 2022/9/24 23:45:04
 */
public interface IHiveTableDdlService {


    /**
     * 确定对应的表有且唯一
     *
     * @param fileName
     * @return java.lang.Integer
     * @author Yav
     * @date 2022-09-25 16:02:33
     */
    Integer validTablesNum(String fileName);

    /**
     * 生成建表语句
     *
     * @param fileName
     * @return org.sanofi.eimadaptor.utils.returnFormat.AirFlowR
     * @author Yav
     * @date 2022-09-25 16:03:05
     */
    Boolean generateTableDdl(String fileName,String jobDate) throws IOException;
    Boolean generateTableDdl(String fileName) throws IOException;


    /**
     * 字段表是否存在
     * @param fileName        
     * @return java.lang.Boolean
     * @author Yav
     * @date 2022-09-25 16:15:54
     */
    Boolean fieldTableExist(String fileName);

    List<String> getTablePk(String fileName);

}
