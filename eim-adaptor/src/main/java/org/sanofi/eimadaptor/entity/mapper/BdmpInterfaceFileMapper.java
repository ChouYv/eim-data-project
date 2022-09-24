package org.sanofi.eimadaptor.entity.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.sanofi.eimadaptor.entity.BdmpInterfaceFile;
import org.springframework.stereotype.Repository;

import java.util.List;


@Mapper
public interface BdmpInterfaceFileMapper extends BaseMapper<BdmpInterfaceFile> {


    @Select("select * from app_upt.bdmp_interface_file where id =76 and coalesce(file_alias,name) = #{inputFileName}" )
    List<BdmpInterfaceFile> selectByNameOrByFileAlias(String inputFileName);
}
