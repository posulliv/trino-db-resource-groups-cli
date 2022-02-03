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

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.exc.UnrecognizedPropertyException;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.resourcegroups.FileResourceGroupConfig;
import io.trino.plugin.resourcegroups.ManagerSpec;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;
import static java.lang.String.format;

public class FileBasedResourceGroups
{
    private static final JsonCodec<ManagerSpec> CODEC = new JsonCodecFactory(
            () -> new ObjectMapperProvider().get().enable(FAIL_ON_UNKNOWN_PROPERTIES))
            .jsonCodec(ManagerSpec.class);

    public static ManagerSpec parseResourceGroupsSchema(String fileName)
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
