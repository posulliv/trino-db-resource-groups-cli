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

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfig;
import org.jdbi.v3.core.Jdbi;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.lang.String.format;

public class ResourceGroupsDbModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DbResourceGroupConfig.class);
        binder.bind(ResourceGroupsDao.class).toProvider(DaoProvider.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    public Jdbi jdbi(DbResourceGroupConfig config)
    {
        loadJdbcDriver(config.getConfigDbUrl());
        return Jdbi.create(config.getConfigDbUrl(), config.getConfigDbUser(), config.getConfigDbPassword());
    }

    // TODO - this seems to be required to guarantee JDBC drivers
    // are loaded. Figure out how to remove  this hack.
    private static void loadJdbcDriver(String configDbUrl)
    {
        if (configDbUrl.startsWith("jdbc:postgresql")) {
            try {
                Class.forName("org.postgresql.Driver");
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        else if (configDbUrl.startsWith("jdbc:oracle")) {
            try {
                Class.forName("oracle.jdbc.driver.Driver");
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        else if (configDbUrl.startsWith("jdbc:mysql")) {
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            }
            catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            throw new IllegalArgumentException(format("Invalid JDBC URL: %s. Only PostgreSQL, MySQL, and Oracle are supported.", configDbUrl));
        }
    }
}
