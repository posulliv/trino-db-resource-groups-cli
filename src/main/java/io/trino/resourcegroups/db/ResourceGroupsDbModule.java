package io.trino.resourcegroups.db;

import com.google.inject.Binder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class ResourceGroupsDbModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DbResourceGroupConfig.class);
    }
}
