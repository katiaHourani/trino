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

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class FileSplitManager
        implements ConnectorSplitManager
{
    private final FileClient fileClient;

    @Inject
    public FileSplitManager(FileClient fileClient)
    {
        this.fileClient = requireNonNull(fileClient, "fileClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        FileTableHandle tableHandle = (FileTableHandle) connectorTableHandle;
        FileTable table = fileClient.getTable(tableHandle.getSchemaName(), tableHandle.getTableName());
        File[] files = fileClient.getTableFiles(table.getPathString());

        List<ConnectorSplit> splits = new ArrayList<>();
        for (File file : files) {
            splits.add(new FileSplit(file.getPath()));
        }
        return new FixedSplitSource(splits);
    }
}
