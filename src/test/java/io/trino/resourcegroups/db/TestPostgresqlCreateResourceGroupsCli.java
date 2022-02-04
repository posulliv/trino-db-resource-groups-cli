/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.resourcegroups.db;

import org.jdbi.v3.core.Handle;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testng.annotations.Test;

@Test(singleThreaded = true)
public class TestPostgresqlCreateResourceGroupsCli
        extends BaseTestCreateResourceGroupsCli
{
    @Override
    protected final JdbcDatabaseContainer<?> startContainer()
    {
        JdbcDatabaseContainer<?> container = new PostgreSQLContainer<>("postgres:9.6");
        container.start();
        return container;
    }

    @Override
    protected final void createAllTables()
    {
        String propertiesTable = "CREATE TABLE resource_groups_global_properties (\n" +
                "    name VARCHAR(128) NOT NULL PRIMARY KEY,\n" +
                "    value VARCHAR(512) NULL,\n" +
                "    CHECK (name in ('cpu_quota_period'))\n" +
                ");";
        String resourceGroupsTable = "CREATE TABLE resource_groups (\n" +
                "    resource_group_id BIGSERIAL PRIMARY KEY,\n" +
                "    name VARCHAR(250) NOT NULL,\n" +
                "    soft_memory_limit VARCHAR(128) NOT NULL,\n" +
                "    max_queued INT NOT NULL,\n" +
                "    soft_concurrency_limit INT NULL,\n" +
                "    hard_concurrency_limit INT NOT NULL,\n" +
                "    scheduling_policy VARCHAR(128) NULL,\n" +
                "    scheduling_weight INT NULL,\n" +
                "    jmx_export BOOLEAN NULL,\n" +
                "    soft_cpu_limit VARCHAR(128) NULL,\n" +
                "    hard_cpu_limit VARCHAR(128) NULL,\n" +
                "    parent BIGINT NULL,\n" +
                "    environment VARCHAR(128) NULL,\n" +
                "    FOREIGN KEY (parent) REFERENCES resource_groups (resource_group_id) ON DELETE CASCADE\n" +
                ");";
        String selectorsTable = "CREATE TABLE selectors (\n" +
                "     resource_group_id BIGINT NOT NULL,\n" +
                "     priority BIGINT NOT NULL,\n" +
                "     user_regex VARCHAR(512),\n" +
                "     source_regex VARCHAR(512),\n" +
                "     query_type VARCHAR(512),\n" +
                "     client_tags VARCHAR(512),\n" +
                "     selector_resource_estimate VARCHAR(1024),\n" +
                "     FOREIGN KEY (resource_group_id) REFERENCES resource_groups (resource_group_id) ON DELETE CASCADE\n" +
                ");";
        Handle jdbiHandle = jdbi.open();
        jdbiHandle.execute(propertiesTable);
        jdbiHandle.execute(resourceGroupsTable);
        jdbiHandle.execute(selectorsTable);
        jdbiHandle.close();
    }

    @Override
    protected final void dropAllTables()
    {
        String propertiesTable = "DROP TABLE IF EXISTS resource_groups_global_properties";
        String resourceGroupsTable = "DROP TABLE IF EXISTS resource_groups";
        String selectorsTable = "DROP TABLE IF EXISTS selectors";
        String exactMatchTable = "DROP TABLE IF EXISTS exact_match_source_selectors";
        Handle jdbiHandle = jdbi.open();
        jdbiHandle.execute(propertiesTable);
        jdbiHandle.execute(selectorsTable);
        jdbiHandle.execute(resourceGroupsTable);
        jdbiHandle.execute(exactMatchTable);
        jdbiHandle.close();
    }
}
