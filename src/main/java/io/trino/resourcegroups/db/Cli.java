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

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.plugin.resourcegroups.ManagerSpec;
import io.trino.plugin.resourcegroups.ResourceGroupIdTemplate;
import io.trino.plugin.resourcegroups.ResourceGroupSpec;
import io.trino.plugin.resourcegroups.SelectorSpec;
import picocli.CommandLine;

import static com.google.common.base.Throwables.throwIfUnchecked;

@CommandLine.Command(
        name = "trino-db-resource-groups-cli",
        usageHelpAutoWidth = true
)
public class Cli
        implements Runnable
{
    private static final Logger LOG = Logger.get(Cli.class);

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @CommandLine.Option(names = "--db-config", required = true, description = "Properties file with resource groups database config")
    public String configFilename;

    @CommandLine.Option(names = "--resource-groups-json", required = true, description = "JSON file with resource groups schema to load")
    public String resourceGroupsSchema;

    @CommandLine.Option(names = "--environment", defaultValue = "test", required = true, description = "Environment where resource groups will be used (matches environment in node.properties")
    public String environment;

    private Cli() {}

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
            LOG.info("Environment to update resource groups for: %s", environment);
            LOG.info("Input JSON file: %s", resourceGroupsSchema);
            ManagerSpec managerSpec = FileBasedResourceGroups.parseResourceGroupsSchema(resourceGroupsSchema);
            ResourceGroupsDao dao = injector.getInstance(ResourceGroupsDao.class);
            dao.setCpuQuotaPeriod(managerSpec.getCpuQuotaPeriod().get().toString());
            // truncating resource_groups table will remove all rows
            // in tables with foreign key constraints on resource_groups
            dao.truncateTable("resource_groups");
            // insert root groups and all children
            for (ResourceGroupSpec resourceGroup : managerSpec.getRootGroups()) {
                dao.insertResourceGroup(resourceGroup, environment, null);
            }
            // userGroup rule in DB selectors is not supported.
            // PR in trino has been opened to add support for this.
            int priority = managerSpec.getSelectors().size();
            for (SelectorSpec selector : managerSpec.getSelectors()) {
                ResourceGroupIdTemplate resourceGroupIdTemplate = selector.getGroup();
                dao.insertSelector(selector, priority);
                priority--;
            }
            LOG.info("Resource groups loaded successfully");
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        finally {
            injector.getInstance(LifeCycleManager.class).stop();
        }
    }

    public static CommandLine create()
    {
        return new CommandLine(new Cli());
    }

    public static void main(String[] args)
    {
        System.exit(create().execute(args));
    }
}
