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

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

import com.oracle.bmc.objectstorage.model.StorageTier;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OciStorageConfigTest {

    private static final String BUCKET_NAME = "test-bucket";
    private static final String NAMESPACE = "test-namespace";
    private static final String REGION = "us-ashburn-1";

    @Test
    void minimalConfig() {
        final var configs = Map.<String, Object>of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION
        );
        final var config = new OciStorageConfig(configs);

        assertThat(config.bucketName()).isEqualTo(BUCKET_NAME);
        assertThat(config.namespaceName()).isEqualTo(NAMESPACE);
        assertThat(config.region().getRegionId()).isEqualTo(REGION);
        assertThat(config.authType()).isEqualTo(OciStorageConfig.AuthType.CONFIG_FILE);
        assertThat(config.uploadPartSize()).isEqualTo(25 * 1024 * 1024);
        assertThat(config.storageTier()).isEqualTo(StorageTier.Standard);
    }

    @Test
    void configWithWorkloadIdentityAuthType() {
        final var configs = Map.<String, Object>of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION,
            "oci.auth.type", "workload_identity"
        );
        final var config = new OciStorageConfig(configs);
        assertThat(config.authType()).isEqualTo(OciStorageConfig.AuthType.WORKLOAD_IDENTITY);
    }

    @Test
    void configWithInstancePrincipalAuthType() {
        final var configs = Map.<String, Object>of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION,
            "oci.auth.type", "instance_principal"
        );
        final var config = new OciStorageConfig(configs);
        assertThat(config.authType()).isEqualTo(OciStorageConfig.AuthType.INSTANCE_PRINCIPAL);
    }

    @Test
    void configWithConfigFileAuthType() {
        final var configs = Map.<String, Object>of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION,
            "oci.auth.type", "config_file",
            "oci.config.file.path", "/some/path/config",
            "oci.config.file.profile", "CUSTOM"
        );
        final var config = new OciStorageConfig(configs);
        assertThat(config.authType()).isEqualTo(OciStorageConfig.AuthType.CONFIG_FILE);
        assertThat(config.getString("oci.config.file.path")).isEqualTo("/some/path/config");
        assertThat(config.getString("oci.config.file.profile")).isEqualTo("CUSTOM");
    }

    @Test
    void configWithCustomPartSize() {
        final int partSize = 10 * 1024 * 1024;
        final var configs = Map.<String, Object>of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION,
            "oci.multipart.upload.part.size", partSize
        );
        final var config = new OciStorageConfig(configs);
        assertThat(config.uploadPartSize()).isEqualTo(partSize);
    }

    @Test
    void configWithStorageTier() {
        final var configs = Map.<String, Object>of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION,
            "oci.storage.tier", "InfrequentAccess"
        );
        final var config = new OciStorageConfig(configs);
        assertThat(config.storageTier()).isEqualTo(StorageTier.InfrequentAccess);
    }

    @Test
    void shouldRequireBucketName() {
        assertThatThrownBy(() -> new OciStorageConfig(Map.of(
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION
        )))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("oci.bucket.name");
    }

    @Test
    void shouldRequireNamespaceName() {
        assertThatThrownBy(() -> new OciStorageConfig(Map.of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.region", REGION
        )))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("oci.namespace.name");
    }

    @Test
    void shouldRequireRegion() {
        assertThatThrownBy(() -> new OciStorageConfig(Map.of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE
        )))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("oci.region");
    }

    @Test
    void shouldRequirePartSizeLargerThan5MiB() {
        assertThatThrownBy(() -> new OciStorageConfig(Map.of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION,
            "oci.multipart.upload.part.size", 1024
        )))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("Value must be at least 5242880");
    }

    @Test
    void shouldRejectInvalidAuthType() {
        assertThatThrownBy(() -> new OciStorageConfig(Map.of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION,
            "oci.auth.type", "invalid_type"
        )))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("oci.auth.type");
    }

    @Test
    void shouldRejectInvalidStorageTier() {
        assertThatThrownBy(() -> new OciStorageConfig(Map.of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION,
            "oci.storage.tier", "WrongTier"
        )))
            .isInstanceOf(ConfigException.class)
            .hasMessageContaining("oci.storage.tier");
    }

    @Test
    void configFileProfileDefaultsToDefault() {
        final var configs = Map.<String, Object>of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION
        );
        final var config = new OciStorageConfig(configs);
        assertThat(config.getString("oci.config.file.profile")).isEqualTo("DEFAULT");
    }

    @Test
    void configFilePathDefaultsToNull() {
        final var configs = Map.<String, Object>of(
            "oci.bucket.name", BUCKET_NAME,
            "oci.namespace.name", NAMESPACE,
            "oci.region", REGION
        );
        final var config = new OciStorageConfig(configs);
        assertThat(config.getString("oci.config.file.path")).isNull();
    }

    @Test
    void authTypeEnumValues() {
        assertThat(OciStorageConfig.AuthType.CONFIG_FILE.configValue()).isEqualTo("config_file");
        assertThat(OciStorageConfig.AuthType.INSTANCE_PRINCIPAL.configValue()).isEqualTo("instance_principal");
        assertThat(OciStorageConfig.AuthType.WORKLOAD_IDENTITY.configValue()).isEqualTo("workload_identity");

        assertThat(OciStorageConfig.AuthType.fromConfigValue("config_file"))
            .isEqualTo(OciStorageConfig.AuthType.CONFIG_FILE);
        assertThat(OciStorageConfig.AuthType.fromConfigValue("instance_principal"))
            .isEqualTo(OciStorageConfig.AuthType.INSTANCE_PRINCIPAL);
        assertThat(OciStorageConfig.AuthType.fromConfigValue("workload_identity"))
            .isEqualTo(OciStorageConfig.AuthType.WORKLOAD_IDENTITY);
    }
}
