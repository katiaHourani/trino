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
package io.trino.plugin.file;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.DistributedQueryRunner;
import io.trino.tpch.TpchTable;

import java.util.List;
import java.util.Map;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;

public final class FileQueryRunner
{
    private static final String CATALOG = "file";

    private FileQueryRunner() {}

    public static DistributedQueryRunner createFileQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> fileProperties,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return builder()
                .setExtraProperties(extraProperties)
                .setInitialTables(tables)
                .setFileProperties(fileProperties)
                .build();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private ImmutableMap.Builder<String, String> fileProperties = ImmutableMap.builder();

        protected Builder()
        {
            super(createSession());
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return self();
        }

        public Builder setFileProperties(Map<String, String> fileProperties)
        {
            this.fileProperties = ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(fileProperties, "fileProperties is null"));
            return self();
        }

        public Builder addFileProperty(String key, String value)
        {
            this.fileProperties.put(key, value);
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();

            try {
                queryRunner.installPlugin(new FilePlugin());
                queryRunner.createCatalog(CATALOG, "file", fileProperties.buildOrThrow());

                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());

                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(), initialTables);

                return queryRunner;
            }
            catch (Exception e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }

        private static Session createSession()
        {
            return testSessionBuilder()
                    .setCatalog(CATALOG)
                    .setSchema("default")
                    .build();
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        DistributedQueryRunner queryRunner = createFileQueryRunner(
                ImmutableMap.of("http-server.http.port", "8080"),
                ImmutableMap.of("file.location", "~/data"),
                TpchTable.getTables());
        Thread.sleep(10);
        Logger log = Logger.get(FileQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
