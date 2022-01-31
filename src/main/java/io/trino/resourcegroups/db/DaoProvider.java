package io.trino.resourcegroups.db;

import io.trino.plugin.resourcegroups.db.DbResourceGroupConfig;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.inject.Inject;
import javax.inject.Provider;

import static java.util.Objects.requireNonNull;

public class DaoProvider
        implements Provider<ResourceGroupsDao>
{
    private final ResourceGroupsDao dao;

    @Inject
    public DaoProvider(DbResourceGroupConfig config, Jdbi jdbi)
    {
        requireNonNull(config, "config is null");
        this.dao = jdbi
                .installPlugin(new SqlObjectPlugin())
                .onDemand(ResourceGroupsDao.class);
    }

    @Override
    public ResourceGroupsDao get()
    {
        return dao;
    }
}
