# Overview

[Trino](https://trino.io) can be configured to load [resource groups](https://trino.io/docs/current/admin/resource-groups.html) from a relational database. In order to create resource groups with this setup, an administrator needs to manually insert data in a set of database tables.

This tool exists to provide a CLI which can take a JSON file with resource group definitions and create the necessary records in the tables read by Trino. The configuration is reloaded from the database every second by Trino, and the changes are reflected automatically for incoming queries.

Trino supports MySQL, PostgreSQL, and Oracle as the database for storing resource group configurations.

# Building & Running

```
mvn clean package
```

This will produce an executable JAR file in the target directory, such as `target/trino-db-resource-groups-cli-1.0-SNAPSHOT-executable.jar`. This file can be moved and renamed to be whatever you like.

```
$ cp target/trino-db-resource-groups-cli-1.0-SNAPSHOT-executable.jar /tmp/trino-db-resource-groups-cli
$ /tmp/trino-db-resource-groups-cli
Missing required subcommand
Usage: trino-db-resource-groups-cli [-h] [COMMAND]
  -h, --help   Show this help message and exit
Commands:
  create_resource_groups
  list_resource_groups
$
```

# Commands

Both commands require the following 2 parameters:

* `db-config` - a properties file with the database configuration
* `environment` - the Trino cluster environment for the resource groups. This should match the value in the `node.properties` file of the cluster you want to change the resource groups for.

The `db-config` parameter takes a properties file that looks like:

```
resource-groups.config-db-url=jdbc:mysql://localhost:3306/resource_groups
resource-groups.config-db-user=trino
resource-groups.config-db-password=trino
```

Any of the properties in this config file can be a [Trino secret](https://trino.io/docs/current/security/secrets.html). For example:

```
resource-groups.config-db-url=jdbc:mysql://localhost:3306/resource_groups
resource-groups.config-db-user=trino
resource-groups.config-db-password=${ENV:MYSQL_PASS}
```

## create_resource_groups

```
trino-db-resource-groups-cli create_resource_groups -h
Usage: trino-db-resource-groups-cli create_resource_groups [-h] --db-config=<configFilename> [--environment=<environment>]
                                                           --resource-groups-json=<resourceGroupsSchema>
      --db-config=<configFilename>
               Properties file with resource groups database config
      --environment=<environment>
               Environment where resource groups will be used (matches environment in node.properties)
  -h, --help   Show this help message and exit
      --resource-groups-json=<resourceGroupsSchema>
               JSON file with resource groups schema to load
```

The input JSON file will be validated to ensure the resource group definitions are correct before inserting anything into the database.

When running this command and everything is successful, you will see output like:

```
$ trino-db-resource-groups-cli create_resource_groups --db-config=resource-groups.properties --resource-groups-json=simple.json --environment=test
2022-02-03T15:48:29.847-0500	INFO	main	io.airlift.log.Logging	Logging to stderr
2022-02-03T15:48:29.849-0500	INFO	main	Bootstrap	Loading configuration
2022-02-03T15:48:30.062-0500	INFO	main	Bootstrap	Initializing logging
2022-02-03T15:48:30.325-0500	INFO	main	Bootstrap	PROPERTY                                      DEFAULT     RUNTIME                                      DESCRIPTION
2022-02-03T15:48:30.326-0500	INFO	main	Bootstrap	resource-groups.config-db-password            [REDACTED]  [REDACTED]                                   Database password
2022-02-03T15:48:30.326-0500	INFO	main	Bootstrap	resource-groups.config-db-url                 ----        jdbc:mysql://localhost:3306/resource_groups
2022-02-03T15:48:30.326-0500	INFO	main	Bootstrap	resource-groups.config-db-user                ----        trino                                        Database user name
2022-02-03T15:48:30.326-0500	INFO	main	Bootstrap	resource-groups.exact-match-selector-enabled  false       false
2022-02-03T15:48:30.326-0500	INFO	main	Bootstrap	resource-groups.max-refresh-interval          1.00h       1.00h                                        Time period for which the cluster will continue to accept queries after refresh failures cause configuration to become stale
2022-02-03T15:48:30.519-0500	INFO	main	io.airlift.bootstrap.LifeCycleManager	Life cycle starting...
2022-02-03T15:48:30.520-0500	INFO	main	io.airlift.bootstrap.LifeCycleManager	Life cycle started
2022-02-03T15:48:30.520-0500	INFO	main	io.trino.resourcegroups.db.CreateResourceGroupsCommand	Environment to update resource groups for: test
2022-02-03T15:48:30.520-0500	INFO	main	io.trino.resourcegroups.db.CreateResourceGroupsCommand	Input JSON file: simple.json
2022-02-03T15:48:31.577-0500	INFO	main	io.trino.resourcegroups.db.CreateResourceGroupsCommand	Resource groups created successfully
2022-02-03T15:48:31.577-0500	INFO	main	io.airlift.bootstrap.LifeCycleManager	Life cycle stopping...
2022-02-03T15:48:31.578-0500	INFO	main	io.airlift.bootstrap.LifeCycleManager	Life cycle stopped
```

## list_resource_groups

```
$ trino-db-resource-groups-cli list_resource_groups --help
Usage: trino-db-resource-groups-cli list_resource_groups [-h] --db-config=<configFilename> [--environment=<environment>]
                                                         --output-json-file=<outputJsonFile>
      --db-config=<configFilename>
               Properties file with resource groups database config
      --environment=<environment>
               Environment where resource groups will be retrieved from (matches environment in node.properties)
  -h, --help   Show this help message and exit
      --output-json-file=<outputJsonFile>
               Path to write JSON file with resource groups schema
$
```

This command will read the current resource groups from the database and write them to a JSON file at the specified path.