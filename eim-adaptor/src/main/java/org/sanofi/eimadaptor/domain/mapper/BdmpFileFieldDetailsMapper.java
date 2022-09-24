package org.sanofi.eimadaptor.domain.mapper;

import com.baomidou.dynamic.datasource.annotation.DS;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Select;
import org.sanofi.eimadaptor.domain.BdmpFileFieldDetails;

import java.util.List;

/**
 * @author yahui
 * @version 1.0
 * @project eim-data-project
 * @description 获取字段相关信息
 * @date 2022/9/24 22:56:28
 */
@Mapper
public interface BdmpFileFieldDetailsMapper {

    @Select("select lower(coalesce(case when field_alias = '' then null else field_alias end, name)) as p_name,\n" +
            "       name,\n" +
            "       field_alias,\n" +
            "       field_type,\n" +
            "       value_range,\n" +
            "       index,\n" +
            "       primary_key,\n" +
            "       business_key,\n" +
            "       fields_comment_en,\n" +
            "       fields_comment_cn,\n" +
            "       if_enum_field,\n" +
            "       if_not_null,\n" +
            "       timestamp_format\n" +
            "from app_upt.bdmp_interface_field\n" +
            "where file_id = #{file_id}\n" +
            "order by index ;")
    List<BdmpFileFieldDetails> selectByFileId(Integer file_id);
}
