package io.trino.resourcegroups.db;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.trino.plugin.resourcegroups.FileResourceGroupConfig;
import io.trino.plugin.resourcegroups.ManagerSpec;
import io.trino.plugin.resourcegroups.db.DbResourceGroupConfig;
import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.String.format;

@CommandLine.Command(
        name = "trino-db-resource-groups-cli",
        usageHelpAutoWidth = true
)
public class Cli
        implements Runnable
{
    private static final Logger LOG = Logger.get(Cli.class);

    private static final JsonCodec<ManagerSpec> CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(ManagerSpec.class);

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @CommandLine.Option(names = "--db-config", required = true, description = "Properties file with resource groups database config")
    public String configFilename;

    @CommandLine.Option(names = "--resource-groups-json", required = true, description = "JSON file with resource groups schema to load")
    public String resourceGroupsSchema;

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
            LOG.info("resource group schema file %s", resourceGroupsSchema);
            ManagerSpec managerSpec = parseResourceGroupsSchema(resourceGroupsSchema);
            LOG.info("we have %d root groups", managerSpec.getRootGroups().size());
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

    private static ManagerSpec parseResourceGroupsSchema(String fileName)
    {
        FileResourceGroupConfig config = new FileResourceGroupConfig();
        config.setConfigFile(fileName);
        ManagerSpec managerSpec;
        try {
            managerSpec = CODEC.fromJson(Files.readAllBytes(Paths.get(config.getConfigFile())));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        catch (IllegalArgumentException e) {
            Throwable cause = e.getCause();
            if (cause instanceof UnrecognizedPropertyException) {
                UnrecognizedPropertyException ex = (UnrecognizedPropertyException) cause;
                String message = format("Unknown property at line %s:%s: %s",
                        ex.getLocation().getLineNr(),
                        ex.getLocation().getColumnNr(),
                        ex.getPropertyName());
                throw new IllegalArgumentException(message, e);
            }
            if (cause instanceof JsonMappingException) {
                // remove the extra "through reference chain" message
                if (cause.getCause() != null) {
                    cause = cause.getCause();
                }
                throw new IllegalArgumentException(cause.getMessage(), e);
            }
            throw e;
        }
        return managerSpec;
    }
}
