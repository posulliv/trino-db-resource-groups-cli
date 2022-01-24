package io.trino.resourcegroups.db;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.log.Logger;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfig;
import picocli.CommandLine;

import java.net.MalformedURLException;

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

    @CommandLine.Parameters(index = "0", paramLabel = "<file>", description = "Configuration file")
    public String configFilename;

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
            DbResourceGroupConfig config = injector.getInstance(DbResourceGroupConfig.class);
            injector.injectMembers(this);
            LOG.info("resource group DB URL %s", config.getConfigDbUrl());
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
