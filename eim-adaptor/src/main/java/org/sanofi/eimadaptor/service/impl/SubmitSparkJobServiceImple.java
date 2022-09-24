package org.sanofi.eimadaptor.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;
import org.sanofi.eimadaptor.service.ISubmitSparkJobService;
import org.sanofi.eimadaptor.utils.http.HttpUtil;
import org.sanofi.eimadaptor.vo.SparkAppParam;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description
 * @date 2022/9/26 15:36:07
 */
@Service
@Slf4j
public class SubmitSparkJobServiceImple implements ISubmitSparkJobService {

    @Value("${driver.name:study}")
    private String driverName;

    @Override
    public String submitJob(SparkAppParam sparkAppParam, String... otherParams) throws IOException, InterruptedException {

        CountDownLatch countDownLatch = new CountDownLatch(1);


        SparkLauncher sparkLauncher = new SparkLauncher()
                .setAppResource(sparkAppParam.getAppResource())
                .setMainClass(sparkAppParam.getMainClass())
                .setMaster(sparkAppParam.getMaster())
                .setDeployMode(sparkAppParam.getDeployMode())
//                .redirectError(new File("/home/zhouyahui/springspark1.log"))
//                .redirectOutput(new File("/home/zhouyahui/springspark.log"))
                .addAppArgs("200","{ \"test\":\"zhouyahui\",\"date\":\"2022-09-28\"}")
                .setConf("spark.driver.memory", sparkAppParam.getDriverMemory())
                .setConf("spark.executor.memory", sparkAppParam.getExecutorMemory())
                .setConf("spark.executor.cores", sparkAppParam.getExecutorCores());

        SparkAppHandle sparkAppHandle = sparkLauncher
                .setVerbose(true)
//                .redirectToLog()
                .startApplication(
                        new SparkAppHandle.Listener() {
                            @Override
                            public void stateChanged(SparkAppHandle handle) {
                                if (handle.getState().isFinal()) {
                                    countDownLatch.countDown();
                                }
                                log.info("stateChanged:{}", handle.getState().toString());
                            }

                            @Override
                            public void infoChanged(SparkAppHandle handle) {
                                log.info("infoChanged:{}", handle.getState().toString());
                            }
                        }
                )
                ;
        countDownLatch.await();
        String restUrl;
        try {
            restUrl = "http://" + driverName + ":18080/api/v1/applications/" + sparkAppHandle.getAppId();
            log.info("访问application运算结果，url:{}", restUrl);
            return HttpUtil.httpGet(restUrl, null);
        } catch (Exception e) {
            log.info("18080端口异常，请确保spark-history-server服务已开启");
            return "{\"state\":\"error\",\"data\":\"history server is not start\"}";
        }

    }
}
