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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.trino.plugin.resourcegroups.ManagerSpec;
import io.trino.plugin.resourcegroups.ResourceGroupIdTemplate;
import io.trino.plugin.resourcegroups.ResourceGroupSpec;
import io.trino.plugin.resourcegroups.SelectorSpec;
import io.trino.plugin.resourcegroups.db.ResourceGroupGlobalProperties;
import io.trino.plugin.resourcegroups.db.ResourceGroupSpecBuilder;
import picocli.CommandLine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;

@CommandLine.Command(
        name = "list_resource_groups",
        usageHelpAutoWidth = true
)
public class ListResourceGroupsCommand
        implements Runnable
{
    private static final Logger LOG = Logger.get(ListResourceGroupsCommand.class);

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @CommandLine.Option(names = "--db-config", required = true, description = "Properties file with resource groups database config")
    public String configFilename;

    @CommandLine.Option(names = "--output-json-file", required = true, description = "Path to write JSON file with resource groups schema")
    public String outputJsonFile;

    @CommandLine.Option(names = "--environment", defaultValue = "test", required = true, description = "Environment where resource groups will be retrieved from (matches environment in node.properties)")
    public String environment;

    private ListResourceGroupsCommand() {}

    @Override
    public void run()
    {
        if (configFilename != null) {
            // Read
            System.setProperty("config", configFilename);
        }

        ImmutableList.Builder<Module> builder = ImmutableList.<Module>builder()
                .add(new ResourceGroupsDbModule())
                .addAll(ImmutableList.of());

        Bootstrap app = new Bootstrap(builder.build());
        Injector injector;
        try {
            injector = app.initialize();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        try {
            injector.injectMembers(this);
            LOG.info("Environment to list resource groups for: %s", environment);
            ResourceGroupsDao dao = injector.getInstance(ResourceGroupsDao.class);
            ManagerSpec managerSpec = loadResourceGroupsFromDb(dao);
            LOG.info("loaded %d root groups", managerSpec.getRootGroups().size());
            LOG.info("loaded %d selectors", managerSpec.getSelectors().size());
            String resourceGroupsJson = ManagerSpecToJson.convert(managerSpec);
            writeJsonToFile(resourceGroupsJson);
            LOG.info("Resource groups written to %s successfully", outputJsonFile);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            injector.getInstance(LifeCycleManager.class).stop();
        }
    }

    private ManagerSpec loadResourceGroupsFromDb(ResourceGroupsDao dao)
    {
        // Set of root group db ids
        Set<Long> rootGroupIds = new HashSet<>();
        // Map of id from db to resource group spec
        Map<Long, ResourceGroupSpec> resourceGroupSpecMap = new HashMap<>();
        // Map of id from db to resource group template id
        Map<Long, ResourceGroupIdTemplate> resourceGroupIdTemplateMap = new HashMap<>();
        // Map of id from db to resource group spec builder
        Map<Long, ResourceGroupSpecBuilder> recordMap = new HashMap<>();
        // Map of subgroup id's not yet built
        Map<Long, Set<Long>> subGroupIdsToBuild = new HashMap<>();
        populateFromDbHelper(dao, recordMap, rootGroupIds, resourceGroupIdTemplateMap, subGroupIdsToBuild);
        // Build up resource group specs from leaf to root
        for (LinkedList<Long> queue = new LinkedList<>(rootGroupIds); !queue.isEmpty(); ) {
            Long id = queue.pollFirst();
            resourceGroupIdTemplateMap.computeIfAbsent(id, k -> {
                ResourceGroupSpecBuilder resourceGroupSpecBuilder = recordMap.get(id);
                return ResourceGroupIdTemplate.forSubGroupNamed(
                        resourceGroupIdTemplateMap.get(resourceGroupSpecBuilder.getParentId().get()),
                        resourceGroupSpecBuilder.getNameTemplate().toString());
            });
            Set<Long> childrenToBuild = subGroupIdsToBuild.getOrDefault(id, ImmutableSet.of());
            // Add to resource group specs if no more child resource groups are left to build
            if (childrenToBuild.isEmpty()) {
                ResourceGroupSpecBuilder specBuilder = recordMap.get(id);
                ResourceGroupSpec resourceGroupSpec = specBuilder.build();
                resourceGroupSpecMap.put(id, resourceGroupSpec);
                // Add this resource group spec to parent subgroups and remove id from subgroup ids to build
                specBuilder.getParentId().ifPresent(parentId -> {
                    recordMap.get(parentId).addSubGroup(resourceGroupSpec);
                    subGroupIdsToBuild.get(parentId).remove(id);
                });
            }
            else {
                // Add this group back to queue since it still has subgroups to build
                queue.addFirst(id);
                // Add this group's subgroups to the queue so that when this id is dequeued again childrenToBuild will be empty
                queue.addAll(0, childrenToBuild);
            }
        }

        // Specs are built from db records, validate and return manager spec
        List<ResourceGroupSpec> rootGroups = rootGroupIds.stream().map(resourceGroupSpecMap::get).collect(Collectors.toList());

        List<SelectorSpec> selectors = dao.getSelectors(environment)
                .stream()
                .map(selectorRecord ->
                        new SelectorSpec(
                                selectorRecord.getUserRegex(),
                                Optional.empty(),
                                selectorRecord.getSourceRegex(),
                                selectorRecord.getQueryType(),
                                selectorRecord.getClientTags(),
                                selectorRecord.getSelectorResourceEstimate(),
                                resourceGroupIdTemplateMap.get(selectorRecord.getResourceGroupId()))
                ).collect(Collectors.toList());
        return new ManagerSpec(rootGroups, selectors, getCpuQuotaPeriodFromDb(dao));
    }

    private void populateFromDbHelper(
            ResourceGroupsDao dao,
            Map<Long, ResourceGroupSpecBuilder> recordMap,
            Set<Long> rootGroupIds,
            Map<Long, ResourceGroupIdTemplate> resourceGroupIdTemplateMap,
            Map<Long, Set<Long>> subGroupIdsToBuild)
    {
        List<ResourceGroupSpecBuilder> records = dao.getResourceGroups(environment);
        for (ResourceGroupSpecBuilder record : records) {
            recordMap.put(record.getId(), record);
            if (record.getParentId().isEmpty()) {
                rootGroupIds.add(record.getId());
                resourceGroupIdTemplateMap.put(record.getId(), new ResourceGroupIdTemplate(record.getNameTemplate().toString()));
            }
            else {
                subGroupIdsToBuild.computeIfAbsent(record.getParentId().get(), k -> new HashSet<>()).add(record.getId());
            }
        }
    }

    private Optional<Duration> getCpuQuotaPeriodFromDb(ResourceGroupsDao dao)
    {
        List<ResourceGroupGlobalProperties> globalProperties = dao.getResourceGroupGlobalProperties();
        checkState(globalProperties.size() <= 1, "There is more than one cpu_quota_period");
        return (!globalProperties.isEmpty()) ? globalProperties.get(0).getCpuQuotaPeriod() : Optional.empty();
    }

    private void writeJsonToFile(String json)
            throws IOException
    {
        File outputFile = new File(outputJsonFile);
        FileWriter fileWriter = new FileWriter(outputFile, false);
        fileWriter.write(json);
        fileWriter.close();
    }
}
