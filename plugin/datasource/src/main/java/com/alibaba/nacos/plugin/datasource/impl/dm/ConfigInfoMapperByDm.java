package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.common.utils.ArrayUtils;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.NamespaceUtil;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.datasource.mapper.ConfigInfoMapper;
import com.alibaba.nacos.plugin.datasource.mapper.ext.WhereBuilder;
import com.alibaba.nacos.plugin.datasource.model.MapperContext;
import com.alibaba.nacos.plugin.datasource.model.MapperResult;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ConfigInfoMapperByDm extends AbstractMapperByDm implements ConfigInfoMapper {
    public MapperResult findConfigInfoByAppFetchRows(MapperContext context) {
        String appName = (String)context.getWhereParameter("app_name");
        String tenantId = (String)context.getWhereParameter("tenantId");
        String sql = "SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,CONTENT FROM CONFIG_INFO WHERE TENANT_ID LIKE ? AND APP_NAME= ? LIMIT " + context.getStartRow() + "," + context.getPageSize();
        return new MapperResult(sql, CollectionUtils.list(new Object[] { tenantId, appName }));
    }

    public MapperResult getTenantIdList(MapperContext context) {
        String sql = "SELECT TENANT_ID FROM CONFIG_INFO WHERE TENANT_ID != '" + NamespaceUtil.getNamespaceDefaultId() + "' GROUP BY TENANT_ID LIMIT " + context.getStartRow() + "," + context.getPageSize();
        return new MapperResult(sql, Collections.emptyList());
    }

    public MapperResult getGroupIdList(MapperContext context) {
        String sql = "SELECT GROUP_ID FROM CONFIG_INFO WHERE TENANT_ID ='" + NamespaceUtil.getNamespaceDefaultId() + "' GROUP BY GROUP_ID LIMIT " + context.getStartRow() + "," + context.getPageSize();
        return new MapperResult(sql, Collections.emptyList());
    }

    public MapperResult findAllConfigKey(MapperContext context) {
        String sql = " SELECT DATA_ID,GROUP_ID,APP_NAME  FROM (  SELECT ID FROM CONFIG_INFO WHERE TENANT_ID LIKE ? ORDER BY ID LIMIT " + context.getStartRow() + "," + context.getPageSize() + " ) G, CONFIG_INFO T WHERE G.ID = T.ID  ";
        return new MapperResult(sql, CollectionUtils.list(new Object[] { context.getWhereParameter("tenantId") }));
    }

    public MapperResult findAllConfigInfoBaseFetchRows(MapperContext context) {
        String sql = "SELECT T.ID,DATA_ID,GROUP_ID,CONTENT,MD5 FROM ( SELECT ID FROM CONFIG_INFO ORDER BY ID LIMIT " + context.getStartRow() + "," + context.getPageSize() + " ) G, CONFIG_INFO T  WHERE G.ID = T.ID ";
        return new MapperResult(sql, Collections.emptyList());
    }

    public MapperResult findAllConfigInfoFragment(MapperContext context) {
        String contextParameter = context.getContextParameter("needContent");
        boolean needContent = (contextParameter != null && Boolean.parseBoolean(contextParameter));
        String sql = "SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME," + (needContent ? "CONTENT," : "") + "MD5,GMT_MODIFIED,TYPE,ENCRYPTED_DATA_KEY FROM CONFIG_INFO WHERE ID > ? ORDER BY ID ASC LIMIT " + context.getStartRow() + "," + context.getPageSize();
        return new MapperResult(sql, CollectionUtils.list(new Object[] { context.getWhereParameter("id") }));
    }

    public MapperResult findChangeConfigFetchRows(MapperContext context) {
        String tenant = (String)context.getWhereParameter("tenantId");
        String dataId = (String)context.getWhereParameter("dataId");
        String group = (String)context.getWhereParameter("groupId");
        String appName = (String)context.getWhereParameter("app_name");
        String tenantTmp = StringUtils.isBlank(tenant) ? "" : tenant;
        Timestamp startTime = (Timestamp)context.getWhereParameter("startTime");
        Timestamp endTime = (Timestamp)context.getWhereParameter("endTime");
        List<Object> paramList = new ArrayList();
        String sqlFetchRows = "SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,TYPE,MD5,GMT_MODIFIED FROM CONFIG_INFO WHERE ";
        String where = " 1=1 ";
        if (!StringUtils.isBlank(dataId)) {
            where = where + " AND DATA_ID LIKE ? ";
            paramList.add(dataId);
        }
        if (!StringUtils.isBlank(group)) {
            where = where + " AND GROUP_ID LIKE ? ";
            paramList.add(group);
        }
        if (!StringUtils.isBlank(tenantTmp)) {
            where = where + " AND TENANT_ID = ? ";
            paramList.add(tenantTmp);
        }
        if (!StringUtils.isBlank(appName)) {
            where = where + " AND APP_NAME = ? ";
            paramList.add(appName);
        }
        if (startTime != null) {
            where = where + " AND GMT_MODIFIED >=? ";
            paramList.add(startTime);
        }
        if (endTime != null) {
            where = where + " AND GMT_MODIFIED <=? ";
            paramList.add(endTime);
        }
        return new MapperResult("SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,TYPE,MD5,GMT_MODIFIED FROM CONFIG_INFO WHERE " + where + " AND ID > " +
                String.valueOf(context.getWhereParameter("lastMaxId")) + " ORDER BY ID ASC LIMIT 0," + context
                .getPageSize(), paramList);
    }

    public MapperResult listGroupKeyMd5ByPageFetchRows(MapperContext context) {
        String sql = "SELECT T.ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,MD5,TYPE,GMT_MODIFIED,ENCRYPTED_DATA_KEY FROM ( SELECT ID FROM CONFIG_INFO ORDER BY ID LIMIT " + context.getStartRow() + "," + context.getPageSize() + " ) G, CONFIG_INFO T WHERE G.ID = T.ID";
        return new MapperResult(sql, Collections.emptyList());
    }

    public MapperResult findConfigInfoBaseLikeFetchRows(MapperContext context) {
        String dataId = (String)context.getWhereParameter("dataId");
        String group = (String)context.getWhereParameter("groupId");
        String content = (String)context.getWhereParameter("content");
        String sqlFetchRows = "SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,CONTENT FROM CONFIG_INFO WHERE ";
        String where = " 1=1 AND TENANT_ID='" + NamespaceUtil.getNamespaceDefaultId() + "' ";
        List<Object> paramList = new ArrayList();
        if (!StringUtils.isBlank(dataId)) {
            where = where + " AND DATA_ID LIKE ? ";
            paramList.add(dataId);
        }
        if (!StringUtils.isBlank(group)) {
            where = where + " AND GROUP_ID LIKE ";
            paramList.add(group);
        }
        if (!StringUtils.isBlank(content)) {
            where = where + " AND CONTENT LIKE ? ";
            paramList.add(content);
        }
        return new MapperResult("SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,CONTENT FROM CONFIG_INFO WHERE " + where + " LIMIT " + context.getStartRow() + "," + context.getPageSize(), paramList);
    }

    public MapperResult findConfigInfo4PageFetchRows(MapperContext context) {
        String tenant = (String)context.getWhereParameter("tenantId");
        String dataId = (String)context.getWhereParameter("dataId");
        String group = (String)context.getWhereParameter("groupId");
        String appName = (String)context.getWhereParameter("app_name");
        String content = (String)context.getWhereParameter("content");
        List<Object> paramList = new ArrayList();
        String sql = "SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,CONTENT,MD5,TYPE,ENCRYPTED_DATA_KEY FROM CONFIG_INFO";
        StringBuilder where = new StringBuilder(" WHERE ");
        where.append(" TENANT_ID=? ");
        paramList.add(tenant);
        if (StringUtils.isNotBlank(dataId)) {
            where.append(" AND DATA_ID=? ");
            paramList.add(dataId);
        }
        if (StringUtils.isNotBlank(group)) {
            where.append(" AND GROUP_ID=? ");
            paramList.add(group);
        }
        if (StringUtils.isNotBlank(appName)) {
            where.append(" AND APP_NAME=? ");
            paramList.add(appName);
        }
        if (!StringUtils.isBlank(content)) {
            where.append(" AND CONTENT LIKE ? ");
            paramList.add(content);
        }
        return new MapperResult("SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,CONTENT,MD5,TYPE,ENCRYPTED_DATA_KEY FROM CONFIG_INFO" + String.valueOf(where) + " LIMIT " + context.getStartRow() + "," + context.getPageSize(), paramList);
    }

    public MapperResult findConfigInfoBaseByGroupFetchRows(MapperContext context) {
        String sql = "SELECT ID,DATA_ID,GROUP_ID,CONTENT FROM CONFIG_INFO WHERE GROUP_ID=? AND TENANT_ID=? LIMIT " + context.getStartRow() + "," + context.getPageSize();
        return new MapperResult(sql, CollectionUtils.list(new Object[] { context.getWhereParameter("groupId"), context
                .getWhereParameter("tenantId") }));
    }

    public MapperResult findConfigInfoLike4PageFetchRows(MapperContext context) {
        String tenant = (String)context.getWhereParameter("tenantId");
        String dataId = (String)context.getWhereParameter("dataId");
        String group = (String)context.getWhereParameter("groupId");
        String appName = (String)context.getWhereParameter("app_name");
        String content = (String)context.getWhereParameter("content");
        String[] types = (String[])context.getWhereParameter("type");
        WhereBuilder where = new WhereBuilder("SELECT ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,CONTENT,MD5,ENCRYPTED_DATA_KEY,TYPE FROM CONFIG_INFO");
        where.like("TENANT_ID", tenant);
        if (StringUtils.isNotBlank(dataId))
            where.and().like("DATA_ID", dataId);
        if (StringUtils.isNotBlank(group))
            where.and().like("GROUP_ID", group);
        if (StringUtils.isNotBlank(appName))
            where.and().eq("APP_NAME", appName);
        if (StringUtils.isNotBlank(content))
            where.and().like("CONTENT", content);
        if (!ArrayUtils.isEmpty((Object[])types))
            where.and().in("TYPE", (Object[])types);
        where.limit(context.getStartRow(), context.getPageSize());
        return where.build();
    }

    public MapperResult findAllConfigInfoFetchRows(MapperContext context) {
        String sql = "SELECT T.ID,DATA_ID,GROUP_ID,TENANT_ID,APP_NAME,CONTENT,MD5  FROM (  SELECT ID FROM CONFIG_INFO WHERE TENANT_ID LIKE ? ORDER BY ID LIMIT ?,? ) G, CONFIG_INFO T  WHERE G.ID = T.ID ";
        return new MapperResult(sql,
                CollectionUtils.list(new Object[] { context.getWhereParameter("tenantId"), Integer.valueOf(context.getStartRow()),
                        Integer.valueOf(context.getPageSize()) }));
    }

    public String getDataSource() {
        return "dm";
    }
}
