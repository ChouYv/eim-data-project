package org.sanofi.eimadaptor.service;

import org.sanofi.eimadaptor.vo.SparkAppParam;

import java.io.IOException;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description 提交spark任务
 * @date 2022/9/26 13:23:52
 */
public interface ISubmitSparkJobService {


    /**
     * 提交sparkJob
     *
     * @param sparkAppParam
     * @param otherParams
     * @return java.lang.String
     * @author Yav
     * @date 2022-09-26 15:35:42
     */
    String submitJob(SparkAppParam sparkAppParam, String... otherParams) throws IOException, InterruptedException;
}
