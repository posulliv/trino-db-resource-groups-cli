package io.trino.resourcegroups.db;

import org.jdbi.v3.sqlobject.SqlObject;

public interface ResourceGroupsDao
        extends SqlObject
{
    default void truncateTable(String tableName)
    {
        useHandle(handle -> {
            handle.createUpdate("DELETE FROM <tableName>")
                    .define("tableName", tableName)
                    .execute();
        });
    }

    default void setCpuQuotaPeriod(String cpuQuotaPeriod)
    {
        // will be the same across all environments
        truncateTable("resource_groups_global_properties");
        useHandle(handle -> {
            handle.createUpdate("INSERT INTO resource_groups_global_properties (name, value) VALUES (:name, :value)")
                    .bind("name", "cpu_quota_period")
                    .bind("value", cpuQuotaPeriod)
                    .execute();
        });
    }
}
