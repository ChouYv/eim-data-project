package org.sanofi.eimadaptor;

import org.junit.jupiter.api.Test;
import org.sanofi.eimadaptor.domain.BdmpFileDetails;
import org.sanofi.eimadaptor.domain.BdmpFileFieldDetails;
import org.sanofi.eimadaptor.entity.BdmpInterfaceFile;
import org.sanofi.eimadaptor.domain.mapper.BdmpFileDetailsMapper;
import org.sanofi.eimadaptor.domain.mapper.BdmpFileFieldDetailsMapper;
import org.sanofi.eimadaptor.entity.mapper.BdmpInterfaceFileMapper;
import org.sanofi.eimadaptor.service.IBdmpInterfaceService;
import org.sanofi.eimadaptor.service.IHiveTableDdlService;
import org.sanofi.eimadaptor.utils.returnFormat.AirFlowR;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SpringBootTest
class EimAdaptorApplicationTests {

    @Test
    void contextLoads() {
    }


    @Autowired
    IBdmpInterfaceService IBdmpInterfaceService;


    @Autowired
    BdmpInterfaceFileMapper BdmpInterfaceFileMapper;


    @Test
    public void test01(){
        System.out.println(IBdmpInterfaceService.getById(1));
        List<BdmpInterfaceFile> md_cl_content = BdmpInterfaceFileMapper.selectByNameOrByFileAlias("MD_CL_CONTENT");
        System.out.println("================================");
        System.out.println(md_cl_content);
        System.out.println("================================");
    }


    @Autowired
    BdmpFileDetailsMapper BdmpFileDetailsMapper;
    //获取文件信息
    @Test
    public void test02(){
        List<BdmpFileDetails> md_cl_content = BdmpFileDetailsMapper.getBdmpFileDetailsByFileAliasOrByName("MD_CL_CONTENT");
        System.out.println(md_cl_content);
    }


    @Autowired
    BdmpFileFieldDetailsMapper BdmpFileFieldDetailsMapper;

    @Test
    public void test03(){

        List<BdmpFileFieldDetails> bdmpFileFieldDetails = BdmpFileFieldDetailsMapper.selectByFileId(1589);
        System.out.println(bdmpFileFieldDetails);
    }




    @Autowired
    IHiveTableDdlService IHiveTableDdlService;
    @Test
    public void test04() throws IOException {
        Boolean md_cl_content = IHiveTableDdlService.generateTableDdl("MD_CL_CONTENT","asd");
//        List<String> md_cl_content = IHiveTableDdlService.getTablePk("MD_CL_CONTENT");
        System.out.println(md_cl_content);
    }




}
