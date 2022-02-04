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
            ManagerSpec managerSpec = DbBasedResourceGroups.loadResourceGroupsFromDb(dao, environment);
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

    private void writeJsonToFile(String json)
            throws IOException
    {
        File outputFile = new File(outputJsonFile);
        FileWriter fileWriter = new FileWriter(outputFile, false);
        fileWriter.write(json);
        fileWriter.close();
    }
}
