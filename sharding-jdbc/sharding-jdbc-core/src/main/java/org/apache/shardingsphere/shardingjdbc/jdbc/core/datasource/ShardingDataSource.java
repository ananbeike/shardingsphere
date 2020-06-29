/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.shardingjdbc.jdbc.adapter.AbstractDataSourceAdapter;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.connection.ShardingConnection;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.context.ShardingRuntimeContext;
import org.apache.shardingsphere.transaction.core.TransactionTypeHolder;

import com.google.common.base.Preconditions;

import lombok.Getter;

/**
 * Sharding data source.
 *
 * 分片数据源对象
 * 
 * 包含：数据源map，运行上下文，
 * private final Map<String, DataSource> dataSourceMap;
 *
 * private final DatabaseType databaseType;
 * 
 * private final ShardingRuntimeContext runtimeContext; 包含：sharding规则，数据库类型，sharding属性配置， sharding执行引擎，sql解析引擎 ，数据库元数据 ，Sharding元数据，sharding事物引擎
 * 
 * @author zhangliang
 * @author zhaojun
 * @author panjuan
 */
@Getter
public class ShardingDataSource extends AbstractDataSourceAdapter{

    private final ShardingRuntimeContext runtimeContext;

    public ShardingDataSource(final Map<String, DataSource> dataSourceMap, final ShardingRule shardingRule, final Properties props) throws SQLException{

        super(dataSourceMap);

        checkDataSourceType(dataSourceMap);

        runtimeContext = new ShardingRuntimeContext(dataSourceMap, shardingRule, props, getDatabaseType());
    }

    private void checkDataSourceType(final Map<String, DataSource> dataSourceMap){
        for (DataSource each : dataSourceMap.values()){
            Preconditions.checkArgument(!(each instanceof MasterSlaveDataSource), "Initialized data sources can not be master-slave data sources.");
        }
    }

    @Override
    public final ShardingConnection getConnection(){
        return new ShardingConnection(getDataSourceMap(), runtimeContext, TransactionTypeHolder.get());
    }
}
