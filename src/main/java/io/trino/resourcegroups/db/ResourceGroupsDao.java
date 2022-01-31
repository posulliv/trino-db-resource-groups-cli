package io.trino.resourcegroups.db;

import io.trino.plugin.resourcegroups.ResourceGroupSpec;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

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

    default void insertResourceGroup(ResourceGroupSpec resourceGroupSpec, String environment, Integer parentId)
    {
        int resourceGroupId = insertResourceGroup(
                resourceGroupSpec.getName().toString(),
                getSoftMemoryLimit(resourceGroupSpec),
                resourceGroupSpec.getMaxQueued(),
                resourceGroupSpec.getSoftConcurrencyLimit().orElse(null),
                resourceGroupSpec.getHardConcurrencyLimit(),
                getSchedulingPolicy(resourceGroupSpec),
                resourceGroupSpec.getSchedulingWeight().orElse(null),
                getJmxExport(resourceGroupSpec),
                getSoftCpuLimit(resourceGroupSpec),
                getHardCpuLimit(resourceGroupSpec),
                parentId,
                environment);
        for (ResourceGroupSpec subGroup : resourceGroupSpec.getSubGroups()) {
            insertResourceGroup(subGroup, environment, Integer.valueOf(resourceGroupId));
        }
    }

    @SqlUpdate("INSERT INTO resource_groups (name, soft_memory_limit, max_queued, soft_concurrency_limit, hard_concurrency_limit, scheduling_policy, scheduling_weight, jmx_export, soft_cpu_limit, hard_cpu_limit, parent, environment) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
    @GetGeneratedKeys("resource_group_id")
    int insertResourceGroup(String name, String softMemoryLimit, int maxQueued, Integer softConcurrencyLimit, int hardConcurrencyLimit, String schedulingPolicy, Integer schedulingWeight, boolean jmxExport, String softCpuLimit, String hardCpuLimit, Integer parent, String environment);

    private String getSoftMemoryLimit(ResourceGroupSpec resourceGroupSpec)
    {
        if (resourceGroupSpec.getSoftMemoryLimitFraction().isPresent()) {
            return resourceGroupSpec.getSoftMemoryLimitFraction().get() * 100 + "%";
        } else if (resourceGroupSpec.getSoftMemoryLimit().isPresent()) {
            return resourceGroupSpec.getSoftMemoryLimit().get().toString();
        }
        return "invalid";
    }

    private String getSoftCpuLimit(ResourceGroupSpec resourceGroupSpec)
    {
        if (resourceGroupSpec.getSoftCpuLimit().isPresent()) {
            return resourceGroupSpec.getSoftCpuLimit().get().toString();
        }
        return null;
    }

    private String getHardCpuLimit(ResourceGroupSpec resourceGroupSpec)
    {
        if (resourceGroupSpec.getHardCpuLimit().isPresent()) {
            return resourceGroupSpec.getHardCpuLimit().get().toString();
        }
        return null;
    }

    private String getSchedulingPolicy(ResourceGroupSpec resourceGroupSpec)
    {
        if (resourceGroupSpec.getSchedulingPolicy().isPresent()) {
            return resourceGroupSpec.getSchedulingPolicy().get().toString();
        }
        return null;
    }

    private boolean getJmxExport(ResourceGroupSpec resourceGroupSpec)
    {
        if (resourceGroupSpec.getJmxExport().isPresent()) {
            return resourceGroupSpec.getJmxExport().get();
        }
        return false;
    }
}
