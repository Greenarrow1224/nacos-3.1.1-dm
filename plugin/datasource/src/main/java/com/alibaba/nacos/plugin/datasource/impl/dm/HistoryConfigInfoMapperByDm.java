package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.mapper.HistoryConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;

public class HistoryConfigInfoMapperByDm extends AbstractMapperByDm implements HistoryConfigInfoMapper {
    public MapperResult removeConfigHistory(MapperContext context) {
        String sql = "DELETE FROM HIS_CONFIG_INFO WHERE GMT_MODIFIED < ? LIMIT ?";
        return new MapperResult(sql, CollectionUtils.list(new Object[] { context.getWhereParameter("startTime"), context
                .getWhereParameter("limitSize") }));
    }

    public MapperResult pageFindConfigHistoryFetchRows(MapperContext context) {
        String sql = "SELECT NID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,SRC_IP,SRC_USER,OP_TYPE,EXT_INFO,PUBLISH_TYPE,GRAY_NAME,GMT_CREATE,GMT_MODIFIED FROM HIS_CONFIG_INFO WHERE DATA_ID = ? AND GROUP_ID = ? AND TENANT_ID = ? ORDER BY NID DESC  LIMIT " + context.getStartRow() + "," + context.getPageSize();
        return new MapperResult(sql, CollectionUtils.list(new Object[] { context.getWhereParameter("dataId"), context
                .getWhereParameter("groupId"), context.getWhereParameter("tenantId") }));
    }

    public String getDataSource() {
        return "dm";
    }
}
