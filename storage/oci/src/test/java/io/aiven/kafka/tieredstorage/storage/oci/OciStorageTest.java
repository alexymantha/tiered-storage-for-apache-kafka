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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

import io.aiven.kafka.tieredstorage.storage.BytesRange;
import io.aiven.kafka.tieredstorage.storage.InvalidRangeException;
import io.aiven.kafka.tieredstorage.storage.KeyNotFoundException;
import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.StorageBackendException;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import com.oracle.bmc.model.BmcException;
import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.DeleteObjectRequest;
import com.oracle.bmc.objectstorage.requests.GetObjectRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.responses.DeleteObjectResponse;
import com.oracle.bmc.objectstorage.responses.GetObjectResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OciStorageTest {

    private static final String NAMESPACE_NAME = "test-namespace";
    private static final String BUCKET_NAME = "test-bucket";
    private static final ObjectKey TEST_KEY = new TestObjectKey("test-object");

    @Mock
    ObjectStorageClient mockedClient;

    @Captor
    ArgumentCaptor<GetObjectRequest> getObjectRequestCaptor;

    @Captor
    ArgumentCaptor<DeleteObjectRequest> deleteObjectRequestCaptor;

    private OciStorage storage;

    @BeforeEach
    void setUp() {
        storage = new OciStorage(mockedClient, NAMESPACE_NAME, BUCKET_NAME, 25 * 1024 * 1024, StorageTier.Standard);
    }

    @Test
    void fetchReturnsInputStream() throws StorageBackendException {
        final byte[] content = {1, 2, 3, 4, 5};
        when(mockedClient.getObject(any(GetObjectRequest.class)))
            .thenReturn(GetObjectResponse.builder()
                .inputStream(new ByteArrayInputStream(content))
                .build());

        final InputStream result = storage.fetch(TEST_KEY);
        verify(mockedClient).getObject(getObjectRequestCaptor.capture());

        assertThat(getObjectRequestCaptor.getValue().getNamespaceName()).isEqualTo(NAMESPACE_NAME);
        assertThat(getObjectRequestCaptor.getValue().getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(getObjectRequestCaptor.getValue().getObjectName()).isEqualTo(TEST_KEY.value());
        assertThat(result).hasBinaryContent(content);
    }

    @Test
    void fetchWithRangeReturnsInputStream() throws StorageBackendException {
        final byte[] content = {2, 3, 4};
        when(mockedClient.getObject(any(GetObjectRequest.class)))
            .thenReturn(GetObjectResponse.builder()
                .inputStream(new ByteArrayInputStream(content))
                .build());

        final BytesRange range = BytesRange.of(1, 3);
        final InputStream result = storage.fetch(TEST_KEY, range);
        verify(mockedClient).getObject(getObjectRequestCaptor.capture());

        assertThat(getObjectRequestCaptor.getValue().getRange().getStartByte()).isEqualTo(1L);
        assertThat(getObjectRequestCaptor.getValue().getRange().getEndByte()).isEqualTo(3L);
        assertThat(result).hasBinaryContent(content);
    }

    @Test
    void fetchWithEmptyRangeReturnsNullInputStream() throws StorageBackendException, IOException {
        final BytesRange range = BytesRange.empty(0);
        final InputStream result = storage.fetch(TEST_KEY, range);
        assertThat(result.readAllBytes()).isEmpty();
    }

    @Test
    void fetchThrowsKeyNotFoundExceptionOn404() {
        when(mockedClient.getObject(any(GetObjectRequest.class)))
            .thenThrow(new BmcException(404, "NotFound", "Object not found", "req-id"));

        assertThatThrownBy(() -> storage.fetch(TEST_KEY))
            .isInstanceOf(KeyNotFoundException.class);
    }

    @Test
    void fetchThrowsStorageBackendExceptionOnOtherErrors() {
        when(mockedClient.getObject(any(GetObjectRequest.class)))
            .thenThrow(new BmcException(500, "InternalError", "Server error", "req-id"));

        assertThatThrownBy(() -> storage.fetch(TEST_KEY))
            .isInstanceOf(StorageBackendException.class)
            .hasMessageContaining("Failed to fetch");
    }

    @Test
    void fetchWithRangeThrowsKeyNotFoundOn404() {
        when(mockedClient.getObject(any(GetObjectRequest.class)))
            .thenThrow(new BmcException(404, "NotFound", "Object not found", "req-id"));

        assertThatThrownBy(() -> storage.fetch(TEST_KEY, BytesRange.of(0, 10)))
            .isInstanceOf(KeyNotFoundException.class);
    }

    @Test
    void fetchWithRangeThrowsInvalidRangeOn416() {
        when(mockedClient.getObject(any(GetObjectRequest.class)))
            .thenThrow(new BmcException(416, "RangeNotSatisfiable", "Invalid range", "req-id"));

        assertThatThrownBy(() -> storage.fetch(TEST_KEY, BytesRange.of(0, 999999)))
            .isInstanceOf(InvalidRangeException.class);
    }

    @Test
    void deleteSingleKey() throws StorageBackendException {
        when(mockedClient.deleteObject(any(DeleteObjectRequest.class)))
            .thenReturn(DeleteObjectResponse.builder().build());

        storage.delete(TEST_KEY);
        verify(mockedClient).deleteObject(deleteObjectRequestCaptor.capture());

        assertThat(deleteObjectRequestCaptor.getValue().getNamespaceName()).isEqualTo(NAMESPACE_NAME);
        assertThat(deleteObjectRequestCaptor.getValue().getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(deleteObjectRequestCaptor.getValue().getObjectName()).isEqualTo(TEST_KEY.value());
    }

    @Test
    void deleteThrowsStorageBackendExceptionOnError() {
        when(mockedClient.deleteObject(any(DeleteObjectRequest.class)))
            .thenThrow(new BmcException(500, "InternalError", "Server error", "req-id"));

        assertThatThrownBy(() -> storage.delete(TEST_KEY))
            .isInstanceOf(StorageBackendException.class)
            .hasMessageContaining("Failed to delete");
    }

    @Test
    void deleteMultipleKeysDeletesSequentially() throws StorageBackendException {
        when(mockedClient.deleteObject(any(DeleteObjectRequest.class)))
            .thenReturn(DeleteObjectResponse.builder().build());

        final ObjectKey key1 = new TestObjectKey("key1");
        final ObjectKey key2 = new TestObjectKey("key2");
        final ObjectKey key3 = new TestObjectKey("key3");
        storage.delete(java.util.Set.of(key1, key2, key3));

        verify(mockedClient, times(3)).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void closeClosesClient() throws IOException {
        storage.close();
        verify(mockedClient).close();
    }

    @Test
    void deleteNonExistentKeyIsIdempotent() throws StorageBackendException {
        when(mockedClient.deleteObject(any(DeleteObjectRequest.class)))
            .thenThrow(new BmcException(404, "NotFound", "Object not found", "req-id"));

        // Should not throw — 404 is silently ignored
        storage.delete(TEST_KEY);
        verify(mockedClient).deleteObject(any(DeleteObjectRequest.class));
    }

    @Test
    void deleteMultipleKeysWithPartialFailure() {
        final ObjectKey key1 = new TestObjectKey("key1");
        final ObjectKey key2 = new TestObjectKey("key2");
        final ObjectKey key3 = new TestObjectKey("key3");

        when(mockedClient.deleteObject(any(DeleteObjectRequest.class)))
            .thenReturn(DeleteObjectResponse.builder().build())
            .thenReturn(DeleteObjectResponse.builder().build())
            .thenThrow(new BmcException(500, "InternalError", "Server error", "req-id"));

        assertThatThrownBy(() -> storage.delete(Set.of(key1, key2, key3)))
            .isInstanceOf(StorageBackendException.class);
    }

    @Test
    void closeWhenClientIsNull() throws IOException {
        final OciStorage unconfigured = new OciStorage();
        // Should not throw — null client is handled gracefully
        unconfigured.close();
    }

    @Test
    void uploadEndToEnd() throws StorageBackendException {
        when(mockedClient.putObject(any(PutObjectRequest.class)))
            .thenReturn(PutObjectResponse.builder().build());

        final byte[] content = {1, 2, 3, 4, 5};
        final long result = storage.upload(new ByteArrayInputStream(content), TEST_KEY);
        assertThat(result).isEqualTo(5);
    }

    @Test
    void toStringContainsFields() {
        assertThat(storage.toString())
            .contains(NAMESPACE_NAME)
            .contains(BUCKET_NAME)
            .contains("Standard");
    }
}
