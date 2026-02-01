package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.plugin.datasource.mapper.TenantInfoMapper;

public class TenantInfoMapperByDm extends AbstractMapperByDm implements TenantInfoMapper {
    public String getDataSource() {
        return "dm";
    }
}
