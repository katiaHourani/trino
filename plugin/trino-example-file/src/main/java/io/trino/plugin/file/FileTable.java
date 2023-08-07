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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;

import java.util.List;

public class FileTable
{
    private String path;
    private final String name;
    private final List<FileColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;

    @JsonCreator
    public FileTable(@JsonProperty("path") String path, @JsonProperty("name") String name, @JsonProperty("columns") List<FileColumn> columns)
    {
        this.path = path;
        this.name = name;
        this.columns = columns;

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (FileColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    @JsonProperty
    public String getPathString()
    {
        return this.path;
    }

    @JsonProperty
    public String getName()
    {
        return this.name;
    }

    @JsonProperty
    public List<FileColumn> getColumns()
    {
        return this.columns;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}
