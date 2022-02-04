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

import picocli.CommandLine;

import java.io.PrintWriter;
import java.io.StringWriter;

import static org.assertj.core.api.Assertions.assertThat;

public final class TestCli
{
    private final int exitCode;
    private final String stdout;
    private final String stderr;

    public static TestCli cli(String... args)
    {
        CommandLine cmd = Cli.create();

        StringWriter stdout = new StringWriter();
        cmd.setOut(new PrintWriter(stdout));
        StringWriter stderr = new StringWriter();
        cmd.setErr(new PrintWriter(stderr));

        int exitCode = cmd.execute(args);

        return new TestCli(exitCode, stdout.toString(), stderr.toString());
    }

    private TestCli(int exitCode, String stdout, String stderr)
    {
        this.exitCode = exitCode;
        this.stdout = stdout;
        this.stderr = stderr;
    }

    public String out()
    {
        assertThat(exitCode).describedAs(stderr).isEqualTo(0);
        return stdout;
    }

    public String err()
    {
        assertThat(exitCode).isNotEqualTo(0);
        return stderr;
    }
}

