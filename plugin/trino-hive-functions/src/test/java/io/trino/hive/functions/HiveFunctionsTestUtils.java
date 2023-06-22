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


package io.trino.hive.functions;
import io.trino.plugin.memory.MemoryPlugin;
import io.trino.server.testing.TestingTrinoServer;

import java.util.Collections;
import java.util.Map;

public final class HiveFunctionsTestUtils
{
    private HiveFunctionsTestUtils() {}

    public static TestingTrinoServer createTestingTrinoServer()
            throws Exception
    {
        TestingTrinoServer server = TestingTrinoServer.builder().build();
        server.installPlugin(new MemoryPlugin());
        server.installPlugin(new HiveFunctionsPlugin());
        server.createCatalog("memory", "memory");
        server.refreshNodes();
        return server;
    }

    public static Map<String, String> getNamespaceManagerCreationProperties()
    {
        return Collections.emptyMap();
    }
}
