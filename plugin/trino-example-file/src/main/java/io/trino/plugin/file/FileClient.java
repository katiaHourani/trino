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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class FileClient
{
    /**
     * SchemaName -> (TableName -> TableMetadata)
     */
    private final Supplier<Map<String, FileSchema>> schemas;
    private final Supplier<BufferedReader> metadataReaderSipplier;

    @Inject
    private FileClient(FileConfig config, JsonCodec<Map<String, List<FileTable>>> catalogCodec, JsonCodec<Map<String, FileSchema>> schemaCatalogCodec)
    {
        requireNonNull(catalogCodec, "catalogCodec is null");
        requireNonNull(schemaCatalogCodec, "catalogCodec is null");
        requireNonNull(config, "config is null");
        metadataReaderSipplier = Suppliers.memoize(metadataReaderSupplier(config.getDataLocation() + "/metadata.json"));
        schemas = Suppliers.memoize(schemasSupplier(schemaCatalogCodec, catalogCodec, config.getDataLocation()));
    }

    public Set<String> getSchemaNames()
    {
        return schemas.get().keySet();
    }

    public Set<String> getTableNames(String schemaName)
    {
        requireNonNull(schemaName, "schema is null");
        FileSchema schema = schemas.get().get(schemaName);
        if (schema == null) {
            return ImmutableSet.of();
        }
        return schema.getTables().keySet();
    }

    private static Supplier<BufferedReader> metadataReaderSupplier(String metadataPath)
    {
        return () -> {
            try {
                return new BufferedReader(new FileReader(metadataPath));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private Supplier<Map<String, FileSchema>> schemasSupplier(JsonCodec<Map<String, FileSchema>> schemaCatalogCodec, JsonCodec<Map<String, List<FileTable>>> catalogCodec, String dataLocation)
    {
        return () -> {
            try {
                return lookupSchemas(dataLocation, catalogCodec, schemaCatalogCodec);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private Map<String, FileSchema> lookupSchemas(String dataLocation, JsonCodec<Map<String, List<FileTable>>> catalogCodec, JsonCodec<Map<String, FileSchema>> schemaCatalogCodec)
            throws IOException
    {
        String json = "";
        String line = this.metadataReaderSipplier.get().readLine();
        // read all lines
        while (line != null) {
            json += line.trim();
            // read next line
            line = this.metadataReaderSipplier.get().readLine();
        }
        return schemaCatalogCodec.fromJson(json);
    }

    public FileTable getTable(String schema, String tableName)
    {
        requireNonNull(schema, "schema is null");
        requireNonNull(tableName, "tableName is null");
        FileSchema fileSchema = schemas.get().get(schema);
        if (fileSchema == null) {
            return null;
        }
        return fileSchema.getTables().get(tableName);
    }

    public File[] getTableFiles(String tablePath)
    {
        File dir = new File(tablePath); // replace with your directory
        System.out.println(tablePath);
        File[] files = dir.listFiles();

        if (files != null) {
            return files;
        }
        else {
            return null;
        }
    }
}
