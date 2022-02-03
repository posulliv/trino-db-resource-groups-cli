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

import io.trino.plugin.resourcegroups.ManagerSpec;
import io.trino.plugin.resourcegroups.ResourceGroupIdTemplate;
import io.trino.plugin.resourcegroups.ResourceGroupNameTemplate;
import io.trino.plugin.resourcegroups.SelectorSpec;

import java.util.List;
import java.util.stream.Collectors;

public class ManagerSpecToJson
{
    private static final String INDENT = "  ";

    public static String convert(ManagerSpec managerSpec)
            throws Exception
    {
        return "{\n" +
                INDENT +
                resourceGroups() +
                ",\n" +
                INDENT +
                selectors(managerSpec.getSelectors()) +
                ",\n" +
                INDENT +
                cpuQuotaPeriod(managerSpec.getCpuQuotaPeriod().get().toString()) +
                "\n}\n";
    }

    private static String resourceGroups()
    {
        return "\"rootGroups\": [\n" +
               indent(2) +
                "{ }\n" +
                indent(1) +
                "]";
    }

    private static String resourceGroup()
    {
        return null;
    }

    private static String selectors(List<SelectorSpec> selectorSpecList)
    {
        return "\"selectors\": [\n" +
                allSelectors(selectorSpecList) +
                "\n" +
                indent(1) +
               "]";
    }

    private static String allSelectors(List<SelectorSpec> selectorSpecList)
    {
        return String.join(
                ",\n",
                selectorSpecList
                        .stream()
                        .map(selectorSpec -> selector(selectorSpec))
                        .collect(Collectors.toList())
        );
    }

    private static String selector(SelectorSpec selectorSpec)
    {
        return indent(2) +
                "{\n" +
                userRegex(selectorSpec) +
                userGroupRegex(selectorSpec) +
                sourceRegex(selectorSpec) +
                queryType(selectorSpec) +
                clientTags(selectorSpec) +
                groupRegex(selectorSpec) +
                indent(2) +
                "}";
    }

    private static String cpuQuotaPeriod(String cpuQuotaPeriod)
    {
        return "\"cpuQuotaPeriod\": \"" + cpuQuotaPeriod + "\"";
    }

    private static String indent(int count)
    {
        return INDENT.repeat(count);
    }

    private static String userRegex(SelectorSpec selectorSpec)
    {
        if (selectorSpec.getUserRegex().isPresent()) {
            return indent(3) + "\"user\": \"" + selectorSpec.getUserRegex().get().toString() + "\",\n";
        }
        return "";
    }

    private static String userGroupRegex(SelectorSpec selectorSpec)
    {
        if (selectorSpec.getUserGroupRegex().isPresent()) {
            return indent(3) + "\"userGroup\": \"" + selectorSpec.getUserGroupRegex().get().toString() + "\",\n";
        }
        return "";
    }

    private static String sourceRegex(SelectorSpec selectorSpec)
    {
        if (selectorSpec.getSourceRegex().isPresent()) {
            return indent(3) + "\"source\": \"" + selectorSpec.getSourceRegex().get().toString() + "\",\n";
        }
        return "";
    }

    private static String queryType(SelectorSpec selectorSpec)
    {
        if (selectorSpec.getQueryType().isPresent()) {
            return indent(3) + "\"queryType\": \"" + selectorSpec.getQueryType().get() + "\",\n";
        }
        return "";
    }

    private static String clientTags(SelectorSpec selectorSpec)
    {
        if (selectorSpec.getClientTags().isPresent()) {
            if (selectorSpec.getClientTags().get().isEmpty()) {
                return "";
            }
            List<String> clientTags = selectorSpec.getClientTags().get();
            String tagsList = String.join(
                    ",",
                    clientTags
                            .stream()
                            .map(clientTag -> ("\"" + clientTag + "\""))
                            .collect(Collectors.toList())
            );
            return indent(3) + "\"clientTags\": [" + tagsList + "],\n";
        }
        return "";
    }

    private static String groupRegex(SelectorSpec selectorSpec)
    {
        ResourceGroupIdTemplate resourceGroupIdTemplate = selectorSpec.getGroup();
        String resourceGroupName = String.join(
                ".",
                resourceGroupIdTemplate.getSegments()
                        .stream()
                        .map(resourceGroupNameTemplate -> resourceGroupNameTemplate.toString())
                        .collect(Collectors.toList())
        );
        return indent(3) + "\"group\": \"" + resourceGroupName + "\"\n";
    }
}
