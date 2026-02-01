package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.common.utils.ArrayUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigTagsRelationMapper;
import com.alibaba.nacos.plugin.datasource.mapper.ext.WhereBuilder;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
import java.util.ArrayList;
import java.util.List;

public class ConfigTagsRelationMapperByDm extends AbstractMapperByDm implements ConfigTagsRelationMapper {
    public MapperResult findConfigInfo4PageFetchRows(MapperContext context) {
        String tenant = (String)context.getWhereParameter("tenantId");
        String dataId = (String)context.getWhereParameter("dataId");
        String group = (String)context.getWhereParameter("groupId");
        String appName = (String)context.getWhereParameter("app_name");
        String content = (String)context.getWhereParameter("content");
        String[] tagArr = (String[])context.getWhereParameter("tagARR");
        List<Object> paramList = new ArrayList();
        StringBuilder where = new StringBuilder(" WHERE ");
        String sql = "SELECT A.ID,A.DATA_ID,A.GROUP_ID,A.TENANT_ID,A.APP_NAME,A.CONTENT FROM CONFIG_INFO  A LEFT JOIN CONFIG_TAGS_RELATION B ON A.ID=B.ID";
        where.append(" A.TENANT_ID=? ");
        paramList.add(tenant);
        if (StringUtils.isNotBlank(dataId)) {
            where.append(" AND A.DATA_ID=? ");
            paramList.add(dataId);
        }
        if (StringUtils.isNotBlank(group)) {
            where.append(" AND A.GROUP_ID=? ");
            paramList.add(group);
        }
        if (StringUtils.isNotBlank(appName)) {
            where.append(" AND A.APP_NAME=? ");
            paramList.add(appName);
        }
        if (!StringUtils.isBlank(content)) {
            where.append(" AND A.CONTENT LIKE ? ");
            paramList.add(content);
        }
        where.append(" AND B.TAG_NAME IN (");
        for (int i = 0; i < tagArr.length; i++) {
            if (i != 0)
                where.append(", ");
            where.append('?');
            paramList.add(tagArr[i]);
        }
        where.append(") ");
        return new MapperResult("SELECT A.ID,A.DATA_ID,A.GROUP_ID,A.TENANT_ID,A.APP_NAME,A.CONTENT FROM CONFIG_INFO  A LEFT JOIN CONFIG_TAGS_RELATION B ON A.ID=B.ID" + String.valueOf(where) + " LIMIT " + context.getStartRow() + "," + context.getPageSize(), paramList);
    }

    public MapperResult findConfigInfoLike4PageFetchRows(MapperContext context) {
        String tenant = (String)context.getWhereParameter("tenantId");
        String dataId = (String)context.getWhereParameter("dataId");
        String group = (String)context.getWhereParameter("groupId");
        String appName = (String)context.getWhereParameter("app_name");
        String content = (String)context.getWhereParameter("content");
        String[] tagArr = (String[])context.getWhereParameter("tagARR");
        String[] types = (String[])context.getWhereParameter("type");
        WhereBuilder where = new WhereBuilder("SELECT A.ID,A.DATA_ID,A.GROUP_ID,A.TENANT_ID,A.APP_NAME,A.CONTENT,A.TYPE FROM CONFIG_INFO A LEFT JOIN CONFIG_TAGS_RELATION B ON A.ID=B.ID");
        where.like("A.TENANT_ID", tenant);
        if (StringUtils.isNotBlank(dataId))
            where.and().like("A.DATA_ID", dataId);
        if (StringUtils.isNotBlank(group))
            where.and().like("A.GROUP_ID", group);
        if (StringUtils.isNotBlank(appName))
            where.and().eq("A.APP_NAME", appName);
        if (StringUtils.isNotBlank(content))
            where.and().like("A.CONTENT", content);
        if (!ArrayUtils.isEmpty((Object[])tagArr)) {
            where.and().startParentheses();
            for (int i = 0; i < tagArr.length; i++) {
                if (i != 0)
                    where.or();
                where.like("B.TAG_NAME", tagArr[i]);
            }
            where.endParentheses();
        }
        if (!ArrayUtils.isEmpty((Object[])types))
            where.and().in("A.TYPE", (Object[])types);
        where.limit(context.getStartRow(), context.getPageSize());
        return where.build();
    }

    public String getDataSource() {
        return "dm";
    }
}
