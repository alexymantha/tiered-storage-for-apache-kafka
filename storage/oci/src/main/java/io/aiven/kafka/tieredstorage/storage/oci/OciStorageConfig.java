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

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import com.oracle.bmc.Region;
import com.oracle.bmc.auth.AbstractAuthenticationDetailsProvider;
import com.oracle.bmc.auth.ConfigFileAuthenticationDetailsProvider;
import com.oracle.bmc.auth.InstancePrincipalsAuthenticationDetailsProvider;
import com.oracle.bmc.auth.okeworkloadidentity.OkeWorkloadIdentityAuthenticationDetailsProvider;
import com.oracle.bmc.objectstorage.model.StorageTier;

public class OciStorageConfig extends AbstractConfig {

    public static final String OCI_NAMESPACE_NAME_CONFIG = "oci.namespace.name";
    private static final String OCI_NAMESPACE_NAME_DOC = "OCI namespace which the bucket belongs to";

    public static final String OCI_BUCKET_NAME_CONFIG = "oci.bucket.name";
    private static final String OCI_BUCKET_NAME_DOC = "OCI bucket to store log segments";

    public static final String OCI_REGION_CONFIG = "oci.region";
    private static final String OCI_REGION_DOC = "OCI region where the bucket is placed";

    public static final String OCI_AUTH_TYPE_CONFIG = "oci.auth.type";
    private static final String OCI_AUTH_TYPE_DOC =
        "Authentication type to use. "
            + "'config_file' uses ~/.oci/config (default, for local development). "
            + "'instance_principal' uses instance principal authentication (for OCI compute instances). "
            + "'workload_identity' uses OKE workload identity (for pods running on OKE).";
    static final String OCI_AUTH_TYPE_DEFAULT = AuthType.CONFIG_FILE.configValue();

    public static final String OCI_CONFIG_FILE_PATH_CONFIG = "oci.config.file.path";
    private static final String OCI_CONFIG_FILE_PATH_DOC =
        "Path to the OCI config file. Only used when oci.auth.type is 'config_file'.";

    public static final String OCI_CONFIG_FILE_PROFILE_CONFIG = "oci.config.file.profile";
    private static final String OCI_CONFIG_FILE_PROFILE_DOC =
        "Profile name in the OCI config file. Only used when oci.auth.type is 'config_file'.";
    static final String OCI_CONFIG_FILE_PROFILE_DEFAULT = "DEFAULT";

    static final String OCI_MULTIPART_UPLOAD_PART_SIZE_CONFIG = "oci.multipart.upload.part.size";
    private static final String OCI_MULTIPART_UPLOAD_PART_SIZE_DOC =
        "Size of parts in bytes to use when uploading. "
            + "All parts but the last one will have this size. "
            + "The smaller the part size, the more calls to OCI are needed to upload a file; increasing costs. "
            + "The higher the part size, the more memory is needed to buffer the part. "
            + "Valid values: between 5MiB and 2GiB";
    static final int OCI_MULTIPART_UPLOAD_PART_SIZE_MIN = 5 * 1024 * 1024; // 5MiB
    static final int OCI_MULTIPART_UPLOAD_PART_SIZE_MAX = Integer.MAX_VALUE;
    static final int OCI_MULTIPART_UPLOAD_PART_SIZE_DEFAULT = 25 * 1024 * 1024; // 25MiB

    public static final String OCI_STORAGE_TIER_CONFIG = "oci.storage.tier";
    private static final String OCI_STORAGE_TIER_DOC =
        "Defines which storage tier to use when uploading objects";
    static final String OCI_STORAGE_TIER_DEFAULT = StorageTier.Standard.toString();

    public static ConfigDef configDef() {
        return new ConfigDef()
            .define(
                OCI_BUCKET_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                OCI_BUCKET_NAME_DOC)
            .define(
                OCI_NAMESPACE_NAME_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                OCI_NAMESPACE_NAME_DOC)
            .define(
                OCI_REGION_CONFIG,
                ConfigDef.Type.STRING,
                ConfigDef.NO_DEFAULT_VALUE,
                new ConfigDef.NonEmptyString(),
                ConfigDef.Importance.HIGH,
                OCI_REGION_DOC)
            .define(
                OCI_AUTH_TYPE_CONFIG,
                ConfigDef.Type.STRING,
                OCI_AUTH_TYPE_DEFAULT,
                ConfigDef.ValidString.in(
                    Arrays.stream(AuthType.values())
                        .map(AuthType::configValue)
                        .toArray(String[]::new)),
                ConfigDef.Importance.HIGH,
                OCI_AUTH_TYPE_DOC)
            .define(
                OCI_CONFIG_FILE_PATH_CONFIG,
                ConfigDef.Type.STRING,
                null,
                ConfigDef.Importance.MEDIUM,
                OCI_CONFIG_FILE_PATH_DOC)
            .define(
                OCI_CONFIG_FILE_PROFILE_CONFIG,
                ConfigDef.Type.STRING,
                OCI_CONFIG_FILE_PROFILE_DEFAULT,
                ConfigDef.Importance.MEDIUM,
                OCI_CONFIG_FILE_PROFILE_DOC)
            .define(
                OCI_MULTIPART_UPLOAD_PART_SIZE_CONFIG,
                ConfigDef.Type.INT,
                OCI_MULTIPART_UPLOAD_PART_SIZE_DEFAULT,
                ConfigDef.Range.between(OCI_MULTIPART_UPLOAD_PART_SIZE_MIN, OCI_MULTIPART_UPLOAD_PART_SIZE_MAX),
                ConfigDef.Importance.MEDIUM,
                OCI_MULTIPART_UPLOAD_PART_SIZE_DOC)
            .define(
                OCI_STORAGE_TIER_CONFIG,
                ConfigDef.Type.STRING,
                OCI_STORAGE_TIER_DEFAULT,
                ConfigDef.ValidString.in(
                    Arrays.stream(StorageTier.values())
                        .map(Object::toString)
                        .toArray(String[]::new)),
                ConfigDef.Importance.MEDIUM,
                OCI_STORAGE_TIER_DOC);
    }

    public OciStorageConfig(final Map<String, ?> props) {
        super(configDef(), props);
    }

    public Region region() {
        return Region.fromRegionId(getString(OCI_REGION_CONFIG));
    }

    public AuthType authType() {
        return AuthType.fromConfigValue(getString(OCI_AUTH_TYPE_CONFIG));
    }

    public AbstractAuthenticationDetailsProvider credentialsProvider() {
        final AuthType type = authType();
        switch (type) {
            case INSTANCE_PRINCIPAL:
                return InstancePrincipalsAuthenticationDetailsProvider.builder().build();
            case WORKLOAD_IDENTITY:
                return OkeWorkloadIdentityAuthenticationDetailsProvider.builder().build();
            case CONFIG_FILE:
                return buildConfigFileProvider();
            default:
                throw new ConfigException("Unknown auth type: " + type);
        }
    }

    private AbstractAuthenticationDetailsProvider buildConfigFileProvider() {
        try {
            final String configFilePath = getString(OCI_CONFIG_FILE_PATH_CONFIG);
            final String profile = getString(OCI_CONFIG_FILE_PROFILE_CONFIG);
            if (configFilePath != null) {
                return new ConfigFileAuthenticationDetailsProvider(configFilePath, profile);
            }
            return new ConfigFileAuthenticationDetailsProvider(profile);
        } catch (final Exception e) {
            throw new ConfigException("Failed to create OCI config file authentication provider: " + e.getMessage());
        }
    }

    public String namespaceName() {
        return getString(OCI_NAMESPACE_NAME_CONFIG);
    }

    public String bucketName() {
        return getString(OCI_BUCKET_NAME_CONFIG);
    }

    public StorageTier storageTier() {
        final String tierValue = getString(OCI_STORAGE_TIER_CONFIG);
        return Arrays.stream(StorageTier.values())
            .filter(st -> st.toString().equals(tierValue))
            .findFirst()
            .orElse(StorageTier.UnknownEnumValue);
    }

    public int uploadPartSize() {
        return getInt(OCI_MULTIPART_UPLOAD_PART_SIZE_CONFIG);
    }

    public enum AuthType {
        CONFIG_FILE,
        INSTANCE_PRINCIPAL,
        WORKLOAD_IDENTITY;

        public String configValue() {
            return name().toLowerCase(Locale.ROOT);
        }

        public static AuthType fromConfigValue(final String value) {
            return AuthType.valueOf(value.toUpperCase(Locale.ROOT));
        }
    }
}
