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

import io.airlift.log.Logger;
import picocli.CommandLine;

import static com.google.common.base.Throwables.throwIfUnchecked;

@CommandLine.Command(
        name = "validate_resource_groups_json",
        usageHelpAutoWidth = true
)
public class ValidateResourceGroupsJsonCommand
        implements Runnable
{
    private static final Logger LOG = Logger.get(CreateResourceGroupsCommand.class);

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @CommandLine.Option(names = "--resource-groups-json", required = true, description = "JSON file with resource groups schema to validate")
    public String resourceGroupsSchema;

    private ValidateResourceGroupsJsonCommand() {}

    @Override
    public void run()
    {
        try {
            LOG.info("JSON file to validate: %s", resourceGroupsSchema);
            FileBasedResourceGroups.parseResourceGroupsSchema(resourceGroupsSchema);
            LOG.info("Resource groups JSON file is valid!");
        }
        catch (IllegalArgumentException iae) {
            LOG.error(iae.getMessage());
            throw new RuntimeException(iae.getMessage());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
