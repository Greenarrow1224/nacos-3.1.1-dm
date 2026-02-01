package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.plugin.datasource.mapper.TenantCapacityMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TenantCapacityMapperByDm extends AbstractMapperByDm implements TenantCapacityMapper {
    public String getDataSource() {
        return "dm";
    }

    public MapperResult select(MapperContext context) {
        String sql = "SELECT ID, QUOTA, `USAGE`, MAX_SIZE, MAX_AGGR_COUNT, MAX_AGGR_SIZE, TENANT_ID FROM TENANT_CAPACITY WHERE TENANT_ID = ?";
        return new MapperResult(sql, Collections.singletonList(context.getWhereParameter("tenantId")));
    }

    public MapperResult getCapacityList4CorrectUsage(MapperContext context) {
        String sql = "SELECT ID, TENANT_ID FROM TENANT_CAPACITY WHERE ID>? LIMIT ?";
        return new MapperResult(sql, CollectionUtils.list(new Object[] { context.getWhereParameter("id"), context
                .getWhereParameter("limitSize") }));
    }

    public MapperResult incrementUsageWithDefaultQuotaLimit(MapperContext context) {
        return new MapperResult("UPDATE TENANT_CAPACITY SET `USAGE` = `USAGE` + 1, GMT_MODIFIED = ? WHERE TENANT_ID = ? AND `USAGE` < ? AND QUOTA = 0",

                CollectionUtils.list(new Object[] { context.getUpdateParameter("gmtModified"), context
                        .getWhereParameter("tenantId"), context
                        .getWhereParameter("usage") }));
    }

    public MapperResult incrementUsageWithQuotaLimit(MapperContext context) {
        return new MapperResult("UPDATE TENANT_CAPACITY SET `USAGE` = `USAGE` + 1, GMT_MODIFIED = ? WHERE TENANT_ID = ? AND `USAGE` < QUOTA AND QUOTA != 0",

                CollectionUtils.list(new Object[] { context.getUpdateParameter("gmtModified"), context
                        .getWhereParameter("tenantId") }));
    }

    public MapperResult incrementUsage(MapperContext context) {
        return new MapperResult("UPDATE TENANT_CAPACITY SET `USAGE` = `USAGE` + 1, GMT_MODIFIED = ? WHERE TENANT_ID = ?",
                CollectionUtils.list(new Object[] { context.getUpdateParameter("gmtModified"), context
                        .getWhereParameter("tenantId") }));
    }

    public MapperResult decrementUsage(MapperContext context) {
        return new MapperResult("UPDATE TENANT_CAPACITY SET `USAGE` = `USAGE` - 1, GMT_MODIFIED = ? WHERE TENANT_ID = ? AND `USAGE` > 0",

                CollectionUtils.list(new Object[] { context.getUpdateParameter("gmtModified"), context
                        .getWhereParameter("tenantId") }));
    }

    public MapperResult correctUsage(MapperContext context) {
        return new MapperResult("UPDATE TENANT_CAPACITY SET `USAGE` = (SELECT COUNT(*) FROM CONFIG_INFO WHERE TENANT_ID = ?), GMT_MODIFIED = ? WHERE TENANT_ID = ?",

                CollectionUtils.list(new Object[] { context.getWhereParameter("tenantId"), context
                        .getUpdateParameter("gmtModified"), context
                        .getWhereParameter("tenantId") }));
    }

    public MapperResult insertTenantCapacity(MapperContext context) {
        List<Object> paramList = new ArrayList();
        paramList.add(context.getUpdateParameter("tenantId"));
        paramList.add(context.getUpdateParameter("quota"));
        paramList.add(context.getUpdateParameter("maxSize"));
        paramList.add(context.getUpdateParameter("maxAggrCount"));
        paramList.add(context.getUpdateParameter("maxAggrSize"));
        paramList.add(context.getUpdateParameter("gmtCreate"));
        paramList.add(context.getUpdateParameter("gmtModified"));
        paramList.add(context.getWhereParameter("tenantId"));
        return new MapperResult("INSERT INTO TENANT_CAPACITY (TENANT_ID, QUOTA, `USAGE`, MAX_SIZE, MAX_AGGR_COUNT, MAX_AGGR_SIZE, GMT_CREATE, GMT_MODIFIED) SELECT ?, ?, COUNT(*), ?, ?, ?, ?, ? FROM CONFIG_INFO WHERE TENANT_ID=?", paramList);
    }
}
