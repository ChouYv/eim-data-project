package org.sanofi.eimadaptor;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


/**
 * @author yahui
 */
@SpringBootApplication
@MapperScan({"org.sanofi.eimadaptor.domain.mapper",
        "org.sanofi.eimadaptor.entity.mapper"})
public class EimAdaptorApplication {


    public static void main(String[] args) {
        SpringApplication.run(EimAdaptorApplication.class, args);
    }

}
