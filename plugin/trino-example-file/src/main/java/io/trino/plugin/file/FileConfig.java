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

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

public class FileConfig
{
    private String dataLocation;

    @NotNull
    public String getDataLocation()
    {
        return dataLocation;
    }

    @Config("file.location")
    public FileConfig setDataLocation(String dataLocation)
    {
        this.dataLocation = dataLocation;
        return this;
    }
}
