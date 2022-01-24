package io.trino.resourcegroups.db;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
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
    private Cli() {}

    @Override
    public void run()
    {
        ImmutableList.Builder<Module> builder = ImmutableList.<Module>builder()
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
        LOG.info("well now mofo");
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
