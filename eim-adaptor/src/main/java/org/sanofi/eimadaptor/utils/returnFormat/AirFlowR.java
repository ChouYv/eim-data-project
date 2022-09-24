package org.sanofi.eimadaptor.utils.returnFormat;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description airflow返回模板
 * @date 2022/9/25 15:05:56
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class AirFlowR<T> implements Serializable {


    private static final long serialVersionUID = -5408579214533798693L;
    private int code;
    private String msg;
    private Object data;


    public static <T> AirFlowR<T> ok(Object data) {
        return result(200, null, data);
    }

    public static <T> AirFlowR<T> ok(String msg, Object data) {
        return result(200, msg, data);
    }

    public static <T> AirFlowR<T> failed() {
        return result(500, null, null);
    }

    public static <T> AirFlowR<T> failed(String msg) {
        return result(500, msg, null);
    }

    public static <T> AirFlowR<T> failed(String msg, Object data) {
        return result(500, msg, data);
    }


    private static <T> AirFlowR<T> result(int code, String msg, Object data) {
        AirFlowR<T> apiResult = new AirFlowR();
        apiResult.setCode(code);
        apiResult.setData(data);
        apiResult.setMsg(msg);
        return apiResult;
    }


}
