package org.sanofi.eimadaptor;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.sanofi.eimadaptor.utils.date.DateUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Date;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description
 * @date 2022/9/26 00:11:16
 */
@Slf4j
public class utilsTest {

    @Test
    void test01(){
        Date date = new Date();
        String s = DateUtils.formatDateTime(date);
        System.out.println(s);

    }

    @Test

    void test02() throws IOException {
        System.out.println("==============================");
        String asd ="application_1665227526638_0001";
        Process process  = Runtime.getRuntime().exec("yarn logs -applicationId "+asd);
        InputStreamReader ir=new InputStreamReader(process.getInputStream());
         LineNumberReader input = new LineNumberReader(ir);
         String line;
        while ((line = input.readLine()) != null) {
            if (line.contains("INFO")) {
                log.info(line);
            } else if (line.contains("WARN")) {
                log.warn(line);
            } else if (line.contains("ERROR")) {
                log.error(line);
            } else if (line.contains("DEBUG")) {
                log.debug(line);
            }
        }
    }
}
