package org.sanofi.eimadaptor.domain.mapper;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.sanofi.eimadaptor.domain.BdmpFileDetails;

import java.util.List;


/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description 获取文件详细数据
 * @date 2022/9/24 22:19:16
 */

@Mapper
public interface BdmpFileDetailsMapper {

    @Select(" select\n" +
            "    bi.id as bi_id,\n" +
            "    bi.source_system as source_system,\n" +
            "    bif.id as bif_id,\n" +
            "    bif.tab_comment_cn,\n" +
            "    bif.load_solution,\n" +
            "    bif.full_delta,\n" +
            "    bif.if_physical_deletion,\n" +
            "    bif.load_key,\n" +
            "    bif.file_type,\n" +
            "    bif.delimiter,\n" +
            "    bif.quote,\n" +
            "    lower(bi.source_system)||'_'||lower(coalesce(bif.file_alias,bif.name)) as ddl_name\n" +
            "from app_upt.bdmp_interface_file bif\n" +
            "         left join app_upt.bdmp_interface bi on bi.id = bif.interface_id\n" +
            "where bi.status=3 and bi.deleted=false and  coalesce(bif.file_alias,bif.name) = #{inputFileName}\n")
    List<BdmpFileDetails> getBdmpFileDetailsByFileAliasOrByName(String inputFileName);

}
