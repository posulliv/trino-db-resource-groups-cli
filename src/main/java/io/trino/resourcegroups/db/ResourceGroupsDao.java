package io.trino.resourcegroups.db;

import io.trino.plugin.resourcegroups.ResourceGroupIdTemplate;
import io.trino.plugin.resourcegroups.ResourceGroupNameTemplate;
import io.trino.plugin.resourcegroups.ResourceGroupSpec;
import io.trino.plugin.resourcegroups.SelectorSpec;
import org.jdbi.v3.sqlobject.SqlObject;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.statement.GetGeneratedKeys;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;

import java.util.List;
import java.util.stream.Collectors;

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

    default void insertSelector(SelectorSpec selectorSpec, int priority)
    {
        int resourceGroupId = getResourceGroupId(selectorSpec.getGroup());
        if (resourceGroupId < 0) {
            // exception should be thrown by getResourceGroupId
        }
        insertSelector(
                resourceGroupId,
                priority,
                getUserRegex(selectorSpec),
                getSourceRegex(selectorSpec),
                selectorSpec.getQueryType().orElse(null),
                getClientTags(selectorSpec),
                null
        );
    }

    @SqlUpdate("INSERT INTO selectors (resource_group_id, priority, user_regex, source_regex, query_type, client_tags, selector_resource_estimate) VALUES (?, ?, ?, ?, ?, ?, ?)")
    void insertSelector(int resourceGroupId, int priority, String userRegex, String sourceRegex, String queryType, String clientTags, String selectorResourceEstimate);

    default int getResourceGroupId(ResourceGroupIdTemplate resourceGroupIdTemplate)
    {
        if (resourceGroupIdTemplate.getSegments().isEmpty()) {
            return -1; // TODO throw exception
        }
        // we need to find the resource group that matches the last segment
        // of the resource group name template.
        ResourceGroupNameTemplate resourceGroupName = resourceGroupIdTemplate.getSegments().get(resourceGroupIdTemplate.getSegments().size() - 1);
        return getResourceGroupId(resourceGroupName.toString());
    }

    @SqlQuery("SELECT resource_group_id\n" +
            "FROM resource_groups\n" +
            "WHERE name = :name\n" +
            "LIMIT 1")
    int getResourceGroupId(@Bind("name") String name);

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

    private String getUserRegex(SelectorSpec selectorSpec)
    {
        if (selectorSpec.getUserRegex().isPresent()) {
            return selectorSpec.getUserRegex().get().toString();
        }
        return null;
    }

    private String getSourceRegex(SelectorSpec selectorSpec)
    {
        if (selectorSpec.getSourceRegex().isPresent()) {
            return selectorSpec.getSourceRegex().get().toString();
        }
        return null;
    }

    private String getClientTags(SelectorSpec selectorSpec)
    {
        if (selectorSpec.getClientTags().isPresent()) {
            if (selectorSpec.getClientTags().get().isEmpty()) {
                return null;
            }
            List<String> clientTags = selectorSpec.getClientTags().get();
            String tagsList = String.join(
                    ",",
                    clientTags
                            .stream()
                            .map(clientTag -> ("\"" + clientTag + "\""))
                            .collect(Collectors.toList())
            );
            return "[" + tagsList + "]";
        }
        return null;
    }
}
