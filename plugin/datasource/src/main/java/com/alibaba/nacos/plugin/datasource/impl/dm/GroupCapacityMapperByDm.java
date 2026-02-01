package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.NamespaceUtil;
import com.alibaba.nacos.plugin.datasource.mapper.GroupCapacityMapper;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GroupCapacityMapperByDm extends AbstractMapperByDm implements GroupCapacityMapper {
    public String getDataSource() {
        return "dm";
    }

    public MapperResult selectGroupInfoBySize(MapperContext context) {
        String sql = "SELECT ID, GROUP_ID FROM GROUP_CAPACITY WHERE ID > ? LIMIT ?";
        return new MapperResult(sql, CollectionUtils.list(new Object[] { context.getWhereParameter("id"), Integer.valueOf(context.getPageSize()) }));
    }

    public MapperResult select(MapperContext context) {
        String sql = "SELECT ID, QUOTA, `USAGE`, MAX_SIZE, MAX_AGGR_COUNT, MAX_AGGR_SIZE, GROUP_ID FROM GROUP_CAPACITY WHERE GROUP_ID = ?";
        return new MapperResult(sql, Collections.singletonList(context.getWhereParameter("groupId")));
    }

    public MapperResult insertIntoSelect(MapperContext context) {
        List<Object> paramList = new ArrayList();
        paramList.add(context.getUpdateParameter("groupId"));
        paramList.add(context.getUpdateParameter("quota"));
        paramList.add(context.getUpdateParameter("maxSize"));
        paramList.add(context.getUpdateParameter("maxAggrCount"));
        paramList.add(context.getUpdateParameter("maxAggrSize"));
        paramList.add(context.getUpdateParameter("gmtCreate"));
        paramList.add(context.getUpdateParameter("gmtModified"));
        String sql = "INSERT INTO GROUP_CAPACITY (GROUP_ID, QUOTA, `USAGE`, MAX_SIZE, MAX_AGGR_COUNT, MAX_AGGR_SIZE,GMT_CREATE, GMT_MODIFIED) SELECT ?, ?, COUNT(*), ?, ?, ?, ?, ? FROM CONFIG_INFO";
        return new MapperResult(sql, paramList);
    }

    public MapperResult insertIntoSelectByWhere(MapperContext context) {
        String sql = "INSERT INTO GROUP_CAPACITY (GROUP_ID, QUOTA, `USAGE`, MAX_SIZE, MAX_AGGR_COUNT, MAX_AGGR_SIZE, GMT_CREATE, GMT_MODIFIED) SELECT ?, ?, COUNT(*), ?, ?, ?, ?, ? FROM CONFIG_INFO WHERE GROUP_ID=? AND TENANT_ID = '" + NamespaceUtil.getNamespaceDefaultId() + "'";
        List<Object> paramList = new ArrayList();
        paramList.add(context.getUpdateParameter("groupId"));
        paramList.add(context.getUpdateParameter("quota"));
        paramList.add(context.getUpdateParameter("maxSize"));
        paramList.add(context.getUpdateParameter("maxAggrCount"));
        paramList.add(context.getUpdateParameter("maxAggrSize"));
        paramList.add(context.getUpdateParameter("gmtCreate"));
        paramList.add(context.getUpdateParameter("gmtModified"));
        paramList.add(context.getWhereParameter("groupId"));
        return new MapperResult(sql, paramList);
    }

    public MapperResult incrementUsageByWhereQuotaEqualZero(MapperContext context) {
        return new MapperResult("UPDATE GROUP_CAPACITY SET `USAGE` = `USAGE` + 1, GMT_MODIFIED = ? WHERE GROUP_ID = ? AND `USAGE` < ? AND QUOTA = 0",

                CollectionUtils.list(new Object[] { context.getUpdateParameter("gmtModified"), context
                        .getWhereParameter("groupId"), context
                        .getWhereParameter("usage") }));
    }

    public MapperResult incrementUsageByWhereQuotaNotEqualZero(MapperContext context) {
        return new MapperResult("UPDATE GROUP_CAPACITY SET `USAGE` = `USAGE` + 1, GMT_MODIFIED = ? WHERE GROUP_ID = ? AND `USAGE` < QUOTA AND QUOTA != 0",

                CollectionUtils.list(new Object[] { context.getUpdateParameter("gmtModified"), context
                        .getWhereParameter("groupId") }));
    }

    public MapperResult incrementUsageByWhere(MapperContext context) {
        return new MapperResult("UPDATE GROUP_CAPACITY SET `USAGE` = `USAGE` + 1, GMT_MODIFIED = ? WHERE GROUP_ID = ?",
                CollectionUtils.list(new Object[] { context.getUpdateParameter("gmtModified"), context
                        .getWhereParameter("groupId") }));
    }

    public MapperResult decrementUsageByWhere(MapperContext context) {
        return new MapperResult("UPDATE GROUP_CAPACITY SET `USAGE` = `USAGE` - 1, GMT_MODIFIED = ? WHERE GROUP_ID = ? AND `USAGE` > 0",

                CollectionUtils.list(new Object[] { context.getUpdateParameter("gmtModified"), context
                        .getWhereParameter("groupId") }));
    }

    public MapperResult updateUsage(MapperContext context) {
        return new MapperResult("UPDATE GROUP_CAPACITY SET `USAGE` = (SELECT COUNT(*) FROM CONFIG_INFO), GMT_MODIFIED = ? WHERE GROUP_ID = ?",

                CollectionUtils.list(new Object[] { context.getUpdateParameter("gmtModified"), context
                        .getWhereParameter("groupId") }));
    }

    public MapperResult updateUsageByWhere(MapperContext context) {
        return new MapperResult("UPDATE GROUP_CAPACITY SET `USAGE` = (SELECT COUNT(*) FROM CONFIG_INFO WHERE GROUP_ID=? AND TENANT_ID = '" +

                NamespaceUtil.getNamespaceDefaultId() + "'), GMT_MODIFIED = ? WHERE GROUP_ID= ?",
                CollectionUtils.list(new Object[] { context.getWhereParameter("groupId"), context
                        .getUpdateParameter("gmtModified"), context
                        .getWhereParameter("groupId") }));
    }
}
