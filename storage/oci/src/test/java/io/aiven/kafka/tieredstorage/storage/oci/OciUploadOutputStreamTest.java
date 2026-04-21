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
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;
import io.aiven.kafka.tieredstorage.storage.TestObjectKey;

import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CommitMultipartUploadPartDetails;
import com.oracle.bmc.objectstorage.model.MultipartUpload;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.AbortMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CommitMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.requests.UploadPartRequest;
import com.oracle.bmc.objectstorage.responses.CommitMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.CreateMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.PutObjectResponse;
import com.oracle.bmc.objectstorage.responses.UploadPartResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OciUploadOutputStreamTest {

    private static final String NAMESPACE_NAME = "test-namespace";
    private static final String BUCKET_NAME = "some_bucket";
    private static final ObjectKey FILE_KEY = new TestObjectKey("some_key");
    private static final String UPLOAD_ID = "some_upload_id";

    @Mock
    ObjectStorageClient mockedClient;

    @Captor
    ArgumentCaptor<CreateMultipartUploadRequest> createMultipartUploadRequest;
    @Captor
    ArgumentCaptor<CommitMultipartUploadRequest> commitMultipartUploadRequestCaptor;
    @Captor
    ArgumentCaptor<AbortMultipartUploadRequest> abortMultipartUploadRequestCaptor;
    @Captor
    ArgumentCaptor<UploadPartRequest> uploadPartRequestCaptor;
    @Captor
    ArgumentCaptor<PutObjectRequest> putObjectRequestCaptor;

    final Random random = new Random();

    @BeforeEach
    void setUp() {
        lenient().when(mockedClient.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
            .thenReturn(newCreateMultipartUploadResponse());
    }

    @Test
    void completeMultipartUploadWithDefaultStorageTier() throws IOException {
        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG#1"));

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 1, StorageTier.Standard, mockedClient);
        out.write(1);

        verify(mockedClient).createMultipartUpload(createMultipartUploadRequest.capture());
        assertThat(createMultipartUploadRequest.getValue().getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(createMultipartUploadRequest.getValue().getNamespaceName()).isEqualTo(NAMESPACE_NAME);
        assertThat(createMultipartUploadRequest.getValue().getCreateMultipartUploadDetails()
            .getStorageTier()).isEqualTo(StorageTier.Standard);
    }

    @Test
    void completeMultipartUploadWithNonDefaultStorageTier() throws IOException {
        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG#1"));

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 1, StorageTier.InfrequentAccess, mockedClient);
        out.write(1);

        verify(mockedClient).createMultipartUpload(createMultipartUploadRequest.capture());
        assertThat(createMultipartUploadRequest.getValue().getCreateMultipartUploadDetails()
            .getStorageTier()).isEqualTo(StorageTier.InfrequentAccess);
    }

    @Test
    void completeMultipartUploadWithUnknownStorageTierOmitsIt() throws IOException {
        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG#1"));

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 1, StorageTier.UnknownEnumValue, mockedClient);
        out.write(1);

        verify(mockedClient).createMultipartUpload(createMultipartUploadRequest.capture());
        assertThat(createMultipartUploadRequest.getValue().getCreateMultipartUploadDetails()
            .getStorageTier()).isNull();
    }

    @Test
    void sendAbortForAnyExceptionWhileWriting() {
        final RuntimeException testException = new RuntimeException("test");
        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenThrow(testException);

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 1, StorageTier.Standard, mockedClient);
        assertThatThrownBy(() -> out.write(new byte[] {1, 2, 3}))
            .isInstanceOf(IOException.class)
            .hasRootCause(testException);

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedClient).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedClient).uploadPart(any(UploadPartRequest.class));
        verify(mockedClient, never()).commitMultipartUpload(any(CommitMultipartUploadRequest.class));
        verify(mockedClient).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());
        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void sendAbortForAnyExceptionWhenClosingUpload() throws Exception {
        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG#1"))
            .thenThrow(RuntimeException.class);

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 10, StorageTier.Standard, mockedClient);

        final byte[] buffer = new byte[15];
        random.nextBytes(buffer);
        out.write(buffer, 0, buffer.length);

        assertThatThrownBy(out::close)
            .isInstanceOf(IOException.class)
            .rootCause()
            .isInstanceOf(RuntimeException.class);

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedClient, never()).commitMultipartUpload(any(CommitMultipartUploadRequest.class));
        verify(mockedClient).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());
        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void sendAbortForAnyExceptionWhenClosingComplete() throws Exception {
        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG#1"));
        when(mockedClient.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
            .thenThrow(RuntimeException.class);

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 10, StorageTier.Standard, mockedClient);

        final byte[] buffer = new byte[10];
        random.nextBytes(buffer);
        out.write(buffer, 0, buffer.length);

        assertThatThrownBy(out::close)
            .isInstanceOf(IOException.class)
            .hasRootCauseInstanceOf(RuntimeException.class);

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedClient).uploadPart(any(UploadPartRequest.class));
        verify(mockedClient).commitMultipartUpload(any(CommitMultipartUploadRequest.class));
        verify(mockedClient).abortMultipartUpload(abortMultipartUploadRequestCaptor.capture());
        assertAbortMultipartUploadRequest(abortMultipartUploadRequestCaptor.getValue());
    }

    @Test
    void writesOnePartUploadByte() throws Exception {
        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenReturn(newUploadPartResponse("SOME_ETAG"));
        when(mockedClient.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
            .thenReturn(CommitMultipartUploadResponse.builder().build());

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 1, StorageTier.Standard, mockedClient);
        out.write(new byte[] {1});
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedClient).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedClient).uploadPart(uploadPartRequestCaptor.capture());
        verify(mockedClient).commitMultipartUpload(commitMultipartUploadRequestCaptor.capture());

        assertUploadPartRequest(uploadPartRequestCaptor.getValue(), 1, 1);
        assertCommitMultipartUploadRequest(
            commitMultipartUploadRequestCaptor.getValue(),
            List.of(CommitMultipartUploadPartDetails.builder().partNum(1).etag("SOME_ETAG").build())
        );
    }

    @Test
    void writesSmallFile() throws Exception {
        when(mockedClient.putObject(any(PutObjectRequest.class)))
            .thenReturn(PutObjectResponse.builder().build());

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 2, StorageTier.Standard, mockedClient);
        out.write(new byte[] {1});
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedClient).putObject(putObjectRequestCaptor.capture());
        verify(mockedClient, never()).commitMultipartUpload(any(CommitMultipartUploadRequest.class));
        verify(mockedClient, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedClient, never()).uploadPart(any(UploadPartRequest.class));

        assertThat(putObjectRequestCaptor.getValue().getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(putObjectRequestCaptor.getValue().getNamespaceName()).isEqualTo(NAMESPACE_NAME);
        assertThat(putObjectRequestCaptor.getValue().getObjectName()).isEqualTo(FILE_KEY.value());
        assertThat(putObjectRequestCaptor.getValue().getContentLength()).isEqualTo(1L);
    }

    @Test
    void writesMultipleMessages() throws Exception {
        final int bufferSize = 10;
        final List<UploadPartRequest> capturedRequests = new ArrayList<>();

        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenAnswer(invocation -> {
                final UploadPartRequest upload = invocation.getArgument(0);
                capturedRequests.add(upload);
                return newUploadPartResponse("SOME_ETAG#" + upload.getUploadPartNum());
            });
        when(mockedClient.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
            .thenReturn(CommitMultipartUploadResponse.builder().build());

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, bufferSize, StorageTier.Standard, mockedClient);
        for (int i = 0; i < 3; i++) {
            final byte[] message = new byte[bufferSize];
            random.nextBytes(message);
            out.write(message, 0, message.length);
        }
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        verify(mockedClient).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedClient, times(3)).uploadPart(any(UploadPartRequest.class));
        verify(mockedClient).commitMultipartUpload(commitMultipartUploadRequestCaptor.capture());

        for (int i = 0; i < 3; i++) {
            assertUploadPartRequest(capturedRequests.get(i), bufferSize, i + 1);
        }

        assertCommitMultipartUploadRequest(
            commitMultipartUploadRequestCaptor.getValue(),
            List.of(
                CommitMultipartUploadPartDetails.builder().partNum(1).etag("SOME_ETAG#1").build(),
                CommitMultipartUploadPartDetails.builder().partNum(2).etag("SOME_ETAG#2").build(),
                CommitMultipartUploadPartDetails.builder().partNum(3).etag("SOME_ETAG#3").build()
            )
        );
    }

    @Test
    void writesTailMessages() throws Exception {
        final int messageSize = 20;
        final List<UploadPartRequest> capturedRequests = new ArrayList<>();

        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenAnswer(invocation -> {
                final UploadPartRequest upload = invocation.getArgument(0);
                capturedRequests.add(upload);
                return newUploadPartResponse("SOME_ETAG#" + upload.getUploadPartNum());
            });
        when(mockedClient.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
            .thenReturn(CommitMultipartUploadResponse.builder().build());

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, messageSize + 10, StorageTier.Standard, mockedClient);
        final byte[] message1 = new byte[messageSize];
        random.nextBytes(message1);
        out.write(message1);
        final byte[] message2 = new byte[messageSize];
        random.nextBytes(message2);
        out.write(message2);
        out.close();

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        assertUploadPartRequest(capturedRequests.get(0), 30, 1);
        assertUploadPartRequest(capturedRequests.get(1), 10, 2);

        verify(mockedClient).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedClient, times(2)).uploadPart(any(UploadPartRequest.class));
        verify(mockedClient, times(1)).commitMultipartUpload(commitMultipartUploadRequestCaptor.capture());

        assertCommitMultipartUploadRequest(
            commitMultipartUploadRequestCaptor.getValue(),
            List.of(
                CommitMultipartUploadPartDetails.builder().partNum(1).etag("SOME_ETAG#1").build(),
                CommitMultipartUploadPartDetails.builder().partNum(2).etag("SOME_ETAG#2").build()
            )
        );
    }

    @Test
    void writesTailMessagesFromInputStreamBufferSmallerThanSize() throws Exception {
        final int messageSize = 10;
        final List<UploadPartRequest> capturedRequests = new ArrayList<>();

        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenAnswer(invocation -> {
                final UploadPartRequest upload = invocation.getArgument(0);
                capturedRequests.add(upload);
                return newUploadPartResponse("SOME_ETAG#" + upload.getUploadPartNum());
            });
        when(mockedClient.commitMultipartUpload(any(CommitMultipartUploadRequest.class)))
            .thenReturn(CommitMultipartUploadResponse.builder().build());

        final byte[] message0 = new byte[messageSize];
        random.nextBytes(message0);
        final byte[] message1 = new byte[messageSize];
        random.nextBytes(message1);
        final var in = new SequenceInputStream(new ByteArrayInputStream(message0), new ByteArrayInputStream(message1));

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 8, StorageTier.Standard, mockedClient);
        try (in; out) {
            in.transferTo(out);
        }

        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        assertUploadPartRequest(capturedRequests.get(0), 8, 1);
        assertUploadPartRequest(capturedRequests.get(1), 8, 2);
        assertUploadPartRequest(capturedRequests.get(2), 4, 3);

        verify(mockedClient).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedClient, times(3)).uploadPart(any(UploadPartRequest.class));
        verify(mockedClient, times(1)).commitMultipartUpload(commitMultipartUploadRequestCaptor.capture());

        assertCommitMultipartUploadRequest(
            commitMultipartUploadRequestCaptor.getValue(),
            List.of(
                CommitMultipartUploadPartDetails.builder().partNum(1).etag("SOME_ETAG#1").build(),
                CommitMultipartUploadPartDetails.builder().partNum(2).etag("SOME_ETAG#2").build(),
                CommitMultipartUploadPartDetails.builder().partNum(3).etag("SOME_ETAG#3").build()
            )
        );
    }

    @Test
    void closeNormallyIfNoWritingHappened() throws IOException {
        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 100, StorageTier.Standard, mockedClient);
        out.close();

        verify(mockedClient, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();
    }

    @Test
    void failWhenUploadingPartAfterStreamIsClosed() throws IOException {
        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 100, StorageTier.Standard, mockedClient);
        out.close();

        verify(mockedClient, never()).abortMultipartUpload(any(AbortMultipartUploadRequest.class));
        assertThat(out.isClosed()).isTrue();
        assertThatCode(out::close).doesNotThrowAnyException();

        assertThatThrownBy(() -> out.write(1))
            .isInstanceOf(IllegalStateException.class)
            .hasMessage("Already closed");
    }

    @Test
    void processedBytesTracked() throws Exception {
        when(mockedClient.putObject(any(PutObjectRequest.class)))
            .thenReturn(PutObjectResponse.builder().build());

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 100, StorageTier.Standard, mockedClient);
        out.write(new byte[] {1, 2, 3});
        out.close();

        assertThat(out.processedBytes()).isEqualTo(3);
    }

    @Test
    void zeroBytesUploadMakesNoApiCalls() throws IOException {
        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 100, StorageTier.Standard, mockedClient);
        out.close();

        verify(mockedClient, never()).putObject(any(PutObjectRequest.class));
        verify(mockedClient, never()).createMultipartUpload(any(CreateMultipartUploadRequest.class));
        verify(mockedClient, never()).uploadPart(any(UploadPartRequest.class));
        verify(mockedClient, never()).commitMultipartUpload(any(CommitMultipartUploadRequest.class));
        assertThat(out.processedBytes()).isEqualTo(0);
    }

    @Test
    void abortFailurePreservesOriginalExceptionAsSuppressed() {
        final RuntimeException uploadError = new RuntimeException("upload failed");
        final RuntimeException abortError = new RuntimeException("abort failed");

        when(mockedClient.uploadPart(any(UploadPartRequest.class)))
            .thenThrow(uploadError);
        when(mockedClient.abortMultipartUpload(any(AbortMultipartUploadRequest.class)))
            .thenThrow(abortError);

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 1, StorageTier.Standard, mockedClient);
        assertThatThrownBy(() -> out.write(new byte[] {1}))
            .isInstanceOf(IOException.class)
            .hasRootCause(uploadError)
            .satisfies(ex -> assertThat(ex.getCause().getSuppressed()).contains(abortError));
    }

    @Test
    void nullStorageTierIsHandled() throws IOException {
        when(mockedClient.putObject(any(PutObjectRequest.class)))
            .thenReturn(PutObjectResponse.builder().build());

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 100, null, mockedClient);
        out.write(new byte[] {1});
        out.close();

        verify(mockedClient).putObject(putObjectRequestCaptor.capture());
        assertThat(putObjectRequestCaptor.getValue().getStorageTier()).isNull();
    }

    @Test
    void storageTierAppliedToSingleFileUpload() throws IOException {
        when(mockedClient.putObject(any(PutObjectRequest.class)))
            .thenReturn(PutObjectResponse.builder().build());

        final var out = new OciUploadOutputStream(
            NAMESPACE_NAME, BUCKET_NAME, FILE_KEY, 100, StorageTier.InfrequentAccess, mockedClient);
        out.write(new byte[] {1});
        out.close();

        verify(mockedClient).putObject(putObjectRequestCaptor.capture());
        assertThat(putObjectRequestCaptor.getValue().getStorageTier()).isEqualTo(StorageTier.InfrequentAccess);
    }

    private static CreateMultipartUploadResponse newCreateMultipartUploadResponse() {
        return CreateMultipartUploadResponse.builder()
            .multipartUpload(MultipartUpload.builder()
                .uploadId(UPLOAD_ID)
                .namespace(NAMESPACE_NAME)
                .bucket(BUCKET_NAME)
                .object(FILE_KEY.value())
                .build())
            .build();
    }

    private static UploadPartResponse newUploadPartResponse(final String etag) {
        return UploadPartResponse.builder().eTag(etag).build();
    }

    private static void assertUploadPartRequest(final UploadPartRequest request,
                                                final long expectedPartSize,
                                                final int expectedPartNumber) {
        assertThat(request.getUploadId()).isEqualTo(UPLOAD_ID);
        assertThat(request.getUploadPartNum()).isEqualTo(expectedPartNumber);
        assertThat(request.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(request.getNamespaceName()).isEqualTo(NAMESPACE_NAME);
        assertThat(request.getObjectName()).isEqualTo(FILE_KEY.value());
        assertThat(request.getContentLength()).isEqualTo(expectedPartSize);
    }

    private static void assertCommitMultipartUploadRequest(
        final CommitMultipartUploadRequest request,
        final List<CommitMultipartUploadPartDetails> expectedParts) {
        assertThat(request.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(request.getNamespaceName()).isEqualTo(NAMESPACE_NAME);
        assertThat(request.getObjectName()).isEqualTo(FILE_KEY.value());
        assertThat(request.getUploadId()).isEqualTo(UPLOAD_ID);
        assertThat(request.getCommitMultipartUploadDetails().getPartsToCommit())
            .containsExactlyElementsOf(expectedParts);
    }

    private static void assertAbortMultipartUploadRequest(final AbortMultipartUploadRequest request) {
        assertThat(request.getBucketName()).isEqualTo(BUCKET_NAME);
        assertThat(request.getNamespaceName()).isEqualTo(NAMESPACE_NAME);
        assertThat(request.getObjectName()).isEqualTo(FILE_KEY.value());
        assertThat(request.getUploadId()).isEqualTo(UPLOAD_ID);
    }
}
