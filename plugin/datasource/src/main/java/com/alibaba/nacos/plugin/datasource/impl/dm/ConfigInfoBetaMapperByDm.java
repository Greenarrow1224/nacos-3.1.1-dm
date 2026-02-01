package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoBetaMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
import java.util.ArrayList;
import java.util.List;

public class ConfigInfoBetaMapperByDm extends AbstractMapperByDm implements ConfigInfoBetaMapper {
    public MapperResult findAllConfigInfoBetaForDumpAllFetchRows(MapperContext context) {
        int startRow = context.getStartRow();
        int pageSize = context.getPageSize();
        String sql = "SELECT T.ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,CONTENT,MD5,GMT_MODIFIED,BETA_IPS,ENCRYPTED_DATA_KEY  FROM ( SELECT ID FROM CONFIG_INFO_BETA  ORDER BY ID LIMIT " + startRow + "," + pageSize + " )  G, CONFIG_INFO_BETA T WHERE G.ID = T.ID ";
        List<Object> paramList = new ArrayList();
        paramList.add(Integer.valueOf(startRow));
        paramList.add(Integer.valueOf(pageSize));
        return new MapperResult(sql, paramList);
    }

    public String getDataSource() {
        return "dm";
    }
}
