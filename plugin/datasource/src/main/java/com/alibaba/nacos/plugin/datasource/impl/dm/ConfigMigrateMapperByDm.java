package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.plugin.datasource.mapper.ConfigMigrateMapper;

public class ConfigMigrateMapperByDm extends AbstractMapperByDm implements ConfigMigrateMapper {
    public String getDataSource() {
        return "dm";
    }
}
