package org.sanofi.eimadaptor.vo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description
 * @date 2022/9/26 15:04:43
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class SparkAppParam {


    /**
     * Sets the application class name for Java/Scala applications.
     *
     * @author Yav
     * @date 2022-09-26 15:15:06
     */
    private String mainClass;

    /**
     * Set the main application resource. This should be the location of a jar file for Scala/Java applications,
     *
     * @author Yav
     * @date 2022-09-26 15:21:16
     */
    private String appResource;
    /**
     * Set the Spark master for the application.
     *
     * @author Yav
     * @date 2022-09-26 15:22:13
     */
    private String master;

    /**
     * Set the deployMode for the application.
     *
     * @author Yav
     * @date 2022-09-26 15:22:29
     */
    private String deployMode;
    /**
     * DRIVER_MEMORY	"spark.driver.memory"
     *
     * @author Yav
     * @date 2022-09-26 15:24:15
     */
    private String driverMemory;
    /**
     * EXECUTOR_MEMORY	"spark.executor.memory"
     *
     * @author Yav
     * @date 2022-09-26 15:24:44
     */
    private String executorMemory;
    /**
     * EXECUTOR_CORES	"spark.executor.cores"
     *
     * @author Yav
     * @date 2022-09-26 15:25:02
     */
    private String executorCores;


    private Map<String, String> extraParams;


    public SparkAppParam getSparkAppParam(){
        return new SparkAppParam(mainClass,appResource,master,deployMode,driverMemory,executorMemory,executorCores,extraParams);
    }

}
