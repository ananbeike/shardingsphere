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

package org.apache.shardingsphere.shardingjdbc.jdbc.core.context;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.shardingsphere.core.config.DatabaseAccessConfiguration;
import org.apache.shardingsphere.core.constant.properties.ShardingPropertiesConstant;
import org.apache.shardingsphere.core.execute.metadata.TableMetaDataInitializer;
import org.apache.shardingsphere.core.metadata.ShardingSphereMetaData;
import org.apache.shardingsphere.core.metadata.datasource.DataSourceMetas;
import org.apache.shardingsphere.core.metadata.table.TableMetas;
import org.apache.shardingsphere.core.rule.ShardingRule;
import org.apache.shardingsphere.shardingjdbc.jdbc.core.datasource.metadata.CachedDatabaseMetaData;
import org.apache.shardingsphere.shardingjdbc.jdbc.metadata.JDBCTableMetaDataConnectionManager;
import org.apache.shardingsphere.spi.database.DatabaseType;
import org.apache.shardingsphere.transaction.ShardingTransactionManagerEngine;

import lombok.Getter;

/**
 * Runtime context for sharding.
 *
 * sharding 运行时上下文
 *
 * 包含：sharding规则，数据库类型，sharding属性配置， sharding执行引擎，sql解析引擎 ，数据库元数据 ，Sharding元数据，sharding事物引擎
 *
 * @author gaohongtao
 * @author panjuan
 * @author zhangliang
 */
@Getter
public final class ShardingRuntimeContext extends AbstractRuntimeContext<ShardingRule>{

    private final DatabaseMetaData cachedDatabaseMetaData;

    /**
     * //TODO 具体数据debug
     */
    private final ShardingSphereMetaData metaData;

    private final ShardingTransactionManagerEngine shardingTransactionManagerEngine;

    /**
     * @param dataSourceMap
     *            所有数据源map
     * @param rule
     *            sharding规则
     * @param props
     *            属性配置
     * @param databaseType
     *            数据库类型
     * @throws SQLException
     */
    public ShardingRuntimeContext(final Map<String, DataSource> dataSourceMap, final ShardingRule rule, final Properties props, final DatabaseType databaseType) throws SQLException{
        //调用父类：初始化【sharding规则，数据库类型，sharding属性配置， sharding执行引擎，sql解析引擎】
        super(rule, props, databaseType);

        //用dataSourceMap中第一个数据源为基础，获取 DatabaseMetaData
        cachedDatabaseMetaData = createCachedDatabaseMetaData(dataSourceMap, rule);

        metaData = createMetaData(dataSourceMap, rule, databaseType);

        shardingTransactionManagerEngine = new ShardingTransactionManagerEngine();
        shardingTransactionManagerEngine.init(databaseType, dataSourceMap);
    }

    /**
     * 用dataSourceMap中第一个数据源为基础，获取 DatabaseMetaData
     *
     * @param dataSourceMap
     * @param rule
     * @return
     * @throws SQLException
     */
    private DatabaseMetaData createCachedDatabaseMetaData(final Map<String, DataSource> dataSourceMap,final ShardingRule rule) throws SQLException{
        //用dataSourceMap中第一个数据源为基础，获取 DatabaseMetaData
        try (Connection connection = dataSourceMap.values().iterator().next().getConnection()){
            return new CachedDatabaseMetaData(connection.getMetaData(), dataSourceMap, rule);
        }
    }

    private ShardingSphereMetaData createMetaData(final Map<String, DataSource> dataSourceMap,final ShardingRule shardingRule,final DatabaseType databaseType) throws SQLException{
        DataSourceMetas dataSourceMetas = new DataSourceMetas(databaseType, getDatabaseAccessConfigurationMap(dataSourceMap));

        TableMetas tableMetas = new TableMetas(getTableMetaDataInitializer(dataSourceMap, dataSourceMetas).load(shardingRule));

        return new ShardingSphereMetaData(dataSourceMetas, tableMetas);
    }

    private Map<String, DatabaseAccessConfiguration> getDatabaseAccessConfigurationMap(final Map<String, DataSource> dataSourceMap) throws SQLException{
        Map<String, DatabaseAccessConfiguration> result = new LinkedHashMap<>(dataSourceMap.size(), 1);
        for (Entry<String, DataSource> entry : dataSourceMap.entrySet()){
            DataSource dataSource = entry.getValue();
            try (Connection connection = dataSource.getConnection()){
                DatabaseMetaData metaData = connection.getMetaData();
                result.put(entry.getKey(), new DatabaseAccessConfiguration(metaData.getURL(), metaData.getUserName(), null));
            }
        }
        return result;
    }

    private TableMetaDataInitializer getTableMetaDataInitializer(final Map<String, DataSource> dataSourceMap,final DataSourceMetas dataSourceMetas){
        return new TableMetaDataInitializer(
                        dataSourceMetas,
                        getExecuteEngine(),
                        new JDBCTableMetaDataConnectionManager(dataSourceMap),
                        this.getProps().<Integer> getValue(ShardingPropertiesConstant.MAX_CONNECTIONS_SIZE_PER_QUERY),
                        this.getProps().<Boolean> getValue(ShardingPropertiesConstant.CHECK_TABLE_METADATA_ENABLED));
    }

    @Override
    public void close() throws Exception{
        shardingTransactionManagerEngine.close();
        super.close();
    }
}
