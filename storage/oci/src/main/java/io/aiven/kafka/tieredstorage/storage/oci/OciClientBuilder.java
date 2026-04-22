/*
 * Copyright 2025 Aiven Oy
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.aiven.kafka.tieredstorage.storage.oci;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.ObjectStorageClient;

class OciClientBuilder {

    // Jersey scans META-INF/services across all classloaders for AutoDiscoverable providers.
    // In a Kafka plugin context, Kafka's parent classloader contains a different version of
    // jersey-server (with WadlAutoDiscoverable) than the plugin's jersey-common (with the
    // AutoDiscoverable interface). The provider gets loaded by the parent classloader and
    // fails to cast to the plugin's interface, causing ClassCastException at startup.
    // Disabling auto-discovery sidesteps the issue entirely; the OCI SDK does not rely on
    // auto-discovered providers.
    static {
        System.setProperty("jersey.config.disableAutoDiscovery", "true");
        System.setProperty("jersey.config.client.disableAutoDiscovery", "true");
    }

    static ObjectStorageClient build(final OciStorageConfig config) {
        final Region region = config.region();
        final AbstractAuthenticationDetailsProvider credentialsProvider = config.credentialsProvider();
        return ObjectStorageClient.builder()
            .region(region)
            .build(credentialsProvider);
    }
}
