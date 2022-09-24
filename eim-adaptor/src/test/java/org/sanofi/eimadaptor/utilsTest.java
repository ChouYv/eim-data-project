package org.sanofi.eimadaptor;

import org.junit.jupiter.api.Test;
import org.sanofi.eimadaptor.utils.date.DateUtils;

import java.util.Date;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description
 * @date 2022/9/26 00:11:16
 */
public class utilsTest {

    @Test
    void test01(){
        Date date = new Date();
        String s = DateUtils.formatDateTime(date);
        System.out.println(s);

    }
}
