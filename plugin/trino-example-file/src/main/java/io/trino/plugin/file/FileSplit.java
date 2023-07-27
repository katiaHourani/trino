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
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class FileSplit
        implements ConnectorSplit
{
    private final String path;
    private final int startLine;
    private final int endLine;
    private final boolean remotelyAccessible;

    @JsonCreator
    public FileSplit(@JsonProperty("path") String path, @JsonProperty("startLine") int startLine, @JsonProperty("endLine") int endLine)
    {
        this.path = requireNonNull(path, "path is null");
        this.startLine = requireNonNull(startLine, "startLine is null");
        this.endLine = requireNonNull(endLine, "endLine is null");
        remotelyAccessible = false;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return remotelyAccessible;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @JsonProperty
    public int getStartLine()
    {
        return startLine;
    }

    @JsonProperty
    public int getEndLine()
    {
        return endLine;
    }
}
