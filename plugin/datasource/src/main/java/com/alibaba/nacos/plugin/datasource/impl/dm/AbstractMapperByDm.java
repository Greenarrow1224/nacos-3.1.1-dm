package com.alibaba.nacos.plugin.datasource.impl.dm;

import com.alibaba.nacos.plugin.datasource.enums.mysql.TrustedMysqlFunctionEnum;
import com.alibaba.nacos.plugin.datasource.mapper.AbstractMapper;

public abstract class AbstractMapperByDm extends AbstractMapper {
    public String getFunction(String functionName) {
        return TrustedMysqlFunctionEnum.getFunctionByName(functionName);
    }
}
