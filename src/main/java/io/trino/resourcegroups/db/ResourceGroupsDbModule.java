package io.trino.resourcegroups.db;

import com.google.inject.Binder;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfig;
import org.jdbi.v3.core.Jdbi;

import static io.airlift.configuration.ConfigBinder.configBinder;

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
        return Jdbi.create(config.getConfigDbUrl(), config.getConfigDbUser(), config.getConfigDbPassword());
    }
}
