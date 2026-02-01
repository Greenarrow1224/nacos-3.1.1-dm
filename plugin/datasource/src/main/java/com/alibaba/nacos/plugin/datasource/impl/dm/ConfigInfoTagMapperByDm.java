package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoTagMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.Collections;

public class ConfigInfoTagMapperByDm extends AbstractMapperByDm implements ConfigInfoTagMapper {
    public MapperResult findAllConfigInfoTagForDumpAllFetchRows(MapperContext context) {
        String sql = " SELECT T.ID,DATA_ID,GROUP_ID,TENANT_ID,TAG_ID,APP_NAME,CONTENT,MD5,GMT_MODIFIED  FROM (  SELECT ID FROM CONFIG_INFO_TAG  ORDER BY ID LIMIT " + context.getStartRow() + "," + context.getPageSize() + " ) G, CONFIG_INFO_TAG T  WHERE G.ID = T.ID  ";
        return new MapperResult(sql, Collections.emptyList());
    }

    public String getDataSource() {
        return "dm";
    }
}
