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
import io.trino.plugin.resourcegroups.ManagerSpec;
import io.trino.plugin.resourcegroups.ResourceGroupSpec;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfig;
import org.jdbi.v3.core.Jdbi;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static com.google.common.io.Resources.getResource;
import static java.nio.file.Files.createTempFile;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public abstract class BaseTestCreateResourceGroupsCli
{
    private static final String ENVIRONMENT = "test";
    protected JdbcDatabaseContainer<?> container;
    protected Jdbi jdbi;
    protected String dbPropertiesFile;

    @BeforeClass
    public final void setup()
            throws IOException
    {
        container = startContainer();
        jdbi = Jdbi.create(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        dbPropertiesFile = createPropertiesFile();
    }

    protected abstract JdbcDatabaseContainer<?> startContainer();

    @AfterClass(alwaysRun = true)
    public final void close()
    {
        container.close();
    }

    @BeforeMethod(alwaysRun = true)
    public void tableSetup()
    {
        createAllTables();
    }

    @AfterMethod(alwaysRun = true)
    public void tableCleanup()
    {
        dropAllTables();
    }

    protected abstract void createAllTables();

    protected abstract void dropAllTables();

    @Test
    public void testInvalidResourceGroups()
    {
        TestCli cli = TestCli.cli(
                "create_resource_groups",
                "--environment=" + ENVIRONMENT,
                "--db-config=" + dbPropertiesFile,
                "--resource-groups-json=" + getResource("invalid_resource_groups.json").getPath()
        );
        assertTrue(cli.err().contains("softMemoryLimit is null"));
    }

    @Test
    public void testSimpleCreate()
    {
        TestCli.cli(
                "create_resource_groups",
                "--environment=" + ENVIRONMENT,
                "--db-config=" + dbPropertiesFile,
                "--resource-groups-json=" + getResource("simple_resource_groups.json").getPath()
        );
        // now verify we have 2 root groups and 2 selectors
        DbResourceGroupConfig config = new DbResourceGroupConfig()
                .setConfigDbUrl(container.getJdbcUrl())
                .setConfigDbUser(container.getUsername())
                .setConfigDbPassword(container.getPassword());
        DaoProvider daoProvider = new DaoProvider(config, jdbi);
        ManagerSpec managerSpec = DbBasedResourceGroups.loadResourceGroupsFromDb(daoProvider.get(), ENVIRONMENT);
        assertEquals(managerSpec.getRootGroups().size(), 2);
        assertEquals(managerSpec.getSelectors().size(), 2);
    }

    @Test
    public void testCreateWithSubgroups()
    {
        TestCli.cli(
                "create_resource_groups",
                "--environment=" + ENVIRONMENT,
                "--db-config=" + dbPropertiesFile,
                "--resource-groups-json=" + getResource("resource_group_with_subgroups.json").getPath()
        );
        // now verify we have 2 root groups and 2 selectors
        DbResourceGroupConfig config = new DbResourceGroupConfig()
                .setConfigDbUrl(container.getJdbcUrl())
                .setConfigDbUser(container.getUsername())
                .setConfigDbPassword(container.getPassword());
        DaoProvider daoProvider = new DaoProvider(config, jdbi);
        ManagerSpec managerSpec = DbBasedResourceGroups.loadResourceGroupsFromDb(daoProvider.get(), ENVIRONMENT);
        assertEquals(managerSpec.getRootGroups().size(), 2);
        assertEquals(getTotalResourceGroupCount(managerSpec.getRootGroups()), 6);
        assertEquals(managerSpec.getSelectors().size(), 4);
    }

    private String createPropertiesFile()
            throws IOException
    {
        List<String> properties = ImmutableList.of(
                "resource-groups.config-db-url=" + container.getJdbcUrl(),
                "resource-groups.config-db-user=" + container.getUsername(),
                "resource-groups.config-db-password=" + container.getPassword()
        );
        Path tmpPath = createTempFile("resource-group-db", ".properties");
        Files.write(tmpPath, properties, TRUNCATE_EXISTING, CREATE, WRITE);
        return tmpPath.toFile().getAbsolutePath();
    }

    private int getTotalResourceGroupCount(List<ResourceGroupSpec> resourceGroups)
    {
        int total = 0;
        for (ResourceGroupSpec resourceGroupSpec : resourceGroups) {
            total++;
            total += getTotalResourceGroupCount(resourceGroupSpec.getSubGroups());
        }
        return total;
    }
}
