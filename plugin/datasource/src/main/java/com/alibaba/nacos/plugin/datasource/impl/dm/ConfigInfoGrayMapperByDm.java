package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoGrayMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

import java.util.Collections;

public class ConfigInfoGrayMapperByDm extends AbstractMapperByDm implements ConfigInfoGrayMapper {
    public MapperResult findAllConfigInfoGrayForDumpAllFetchRows(MapperContext context) {
        String sql = " SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,GRAY_NAME,GRAY_RULE,APP_NAME,CONTENT,MD5,GMT_MODIFIED  FROM  CONFIG_INFO_GRAY  ORDER BY ID LIMIT " + context.getStartRow() + "," + context.getPageSize();
        return new MapperResult(sql, Collections.emptyList());
    }

    public String getDataSource() {
        return "dm";
    }
}

