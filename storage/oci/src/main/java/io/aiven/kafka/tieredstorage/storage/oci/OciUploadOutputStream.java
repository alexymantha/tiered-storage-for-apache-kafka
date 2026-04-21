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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import io.aiven.kafka.tieredstorage.storage.ObjectKey;

import com.oracle.bmc.objectstorage.ObjectStorageClient;
import com.oracle.bmc.objectstorage.model.CommitMultipartUploadDetails;
import com.oracle.bmc.objectstorage.model.CommitMultipartUploadPartDetails;
import com.oracle.bmc.objectstorage.model.CreateMultipartUploadDetails;
import com.oracle.bmc.objectstorage.model.StorageTier;
import com.oracle.bmc.objectstorage.requests.AbortMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CommitMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.CreateMultipartUploadRequest;
import com.oracle.bmc.objectstorage.requests.PutObjectRequest;
import com.oracle.bmc.objectstorage.requests.UploadPartRequest;
import com.oracle.bmc.objectstorage.responses.CreateMultipartUploadResponse;
import com.oracle.bmc.objectstorage.responses.UploadPartResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OCI Object Storage output stream.
 * Enable uploads to OCI with unknown size by feeding input bytes to multiple parts or single file and upload.
 *
 * <p>Requires OCI ObjectStorageClient and starts a multipart transaction when sending file over upload part size.
 * Do not reuse.
 *
 * <p>{@link OciUploadOutputStream} is not thread-safe.
 */
public class OciUploadOutputStream extends OutputStream {

    private static final Logger log = LoggerFactory.getLogger(OciUploadOutputStream.class);

    private final ObjectStorageClient client;
    private final ByteBuffer partBuffer;
    private final String namespaceName;
    private final String bucketName;
    private final ObjectKey key;
    private final StorageTier storageTier;
    final int partSize;

    private String uploadId;
    private final List<CommitMultipartUploadPartDetails> completedParts = new ArrayList<>();

    private boolean closed;
    private long processedBytes;

    public OciUploadOutputStream(final String namespaceName,
                                 final String bucketName,
                                 final ObjectKey key,
                                 final int partSize,
                                 final StorageTier storageTier,
                                 final ObjectStorageClient client) {
        this.namespaceName = namespaceName;
        this.bucketName = bucketName;
        this.key = key;
        this.storageTier = storageTier;
        this.client = client;
        this.partSize = partSize;
        this.partBuffer = ByteBuffer.allocate(partSize);
    }

    @Override
    public void write(final int b) throws IOException {
        write(new byte[] {(byte) b}, 0, 1);
    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if (isClosed()) {
            throw new IllegalStateException("Already closed");
        }
        if (b.length == 0) {
            return;
        }
        try {
            final ByteBuffer inputBuffer = ByteBuffer.wrap(b, off, len);
            while (inputBuffer.hasRemaining()) {
                // copy batch to part buffer
                final int inputLimit = inputBuffer.limit();
                final int toCopy = Math.min(partBuffer.remaining(), inputBuffer.remaining());
                final int positionAfterCopying = inputBuffer.position() + toCopy;
                inputBuffer.limit(positionAfterCopying);
                partBuffer.put(inputBuffer.slice());

                // prepare current batch for next part
                inputBuffer.limit(inputLimit);
                inputBuffer.position(positionAfterCopying);

                if (!partBuffer.hasRemaining()) {
                    if (uploadId == null) {
                        uploadId = createMultipartUpload();
                        if (uploadId == null || uploadId.isEmpty()) {
                            throw new IOException("Failed to create multipart upload, uploadId is empty");
                        }
                    }
                    partBuffer.clear();
                    flushBuffer(partBuffer.slice(), partSize, true);
                }
            }
        } catch (final RuntimeException e) {
            closed = true;
            if (multiPartUploadStarted()) {
                log.error("Failed to write to stream on upload {}, aborting transaction", uploadId, e);
                tryAbortUpload(e);
            }
            throw new IOException(e);
        }
    }

    private String createMultipartUpload() {
        final CreateMultipartUploadDetails.Builder detailsBuilder = CreateMultipartUploadDetails.builder()
            .object(key.value())
            .contentType("application/octet-stream");
        if (storageTierDefined()) {
            detailsBuilder.storageTier(storageTier);
        }
        final CreateMultipartUploadRequest initialRequest = CreateMultipartUploadRequest.builder()
            .namespaceName(namespaceName)
            .bucketName(bucketName)
            .createMultipartUploadDetails(detailsBuilder.build())
            .build();
        final CreateMultipartUploadResponse response = client.createMultipartUpload(initialRequest);
        final String id = response.getMultipartUpload().getUploadId();
        log.debug("Create new multipart upload request: {}", id);
        return id;
    }

    private boolean multiPartUploadStarted() {
        return uploadId != null;
    }

    @Override
    public void close() throws IOException {
        if (!isClosed()) {
            closed = true;
            final int lastPosition = partBuffer.position();
            if (lastPosition > 0) {
                try {
                    partBuffer.position(0);
                    partBuffer.limit(lastPosition);
                    flushBuffer(partBuffer.slice(), lastPosition, multiPartUploadStarted());
                } catch (final RuntimeException e) {
                    if (multiPartUploadStarted()) {
                        log.error("Failed to upload last part {}, aborting transaction", uploadId, e);
                        tryAbortUpload(e);
                    } else {
                        log.error("Failed to upload the file {}", key, e);
                    }
                    throw new IOException(e);
                }
            }
            if (multiPartUploadStarted()) {
                completeOrAbortMultiPartUpload();
            }
        }
    }

    private void completeOrAbortMultiPartUpload() throws IOException {
        if (!completedParts.isEmpty()) {
            try {
                completeUpload();
                log.debug("Completed multipart upload {}", uploadId);
            } catch (final RuntimeException e) {
                log.error("Failed to complete multipart upload {}, aborting transaction", uploadId, e);
                tryAbortUpload(e);
                throw new IOException(e);
            }
        } else {
            abortUpload();
        }
    }

    private void uploadAsSingleFile(final InputStream inputStream, final int size) {
        final PutObjectRequest.Builder builder = PutObjectRequest.builder()
            .namespaceName(namespaceName)
            .bucketName(bucketName)
            .objectName(key.value())
            .contentLength((long) size)
            .putObjectBody(inputStream);
        if (storageTierDefined()) {
            builder.storageTier(storageTier);
        }
        client.putObject(builder.build());
    }

    public boolean isClosed() {
        return closed;
    }

    private void completeUpload() {
        final CommitMultipartUploadDetails commitDetails = CommitMultipartUploadDetails.builder()
            .partsToCommit(completedParts)
            .build();
        final CommitMultipartUploadRequest request = CommitMultipartUploadRequest.builder()
            .namespaceName(namespaceName)
            .bucketName(bucketName)
            .objectName(key.value())
            .uploadId(uploadId)
            .commitMultipartUploadDetails(commitDetails)
            .build();
        client.commitMultipartUpload(request);
    }

    private void abortUpload() {
        final AbortMultipartUploadRequest request = AbortMultipartUploadRequest.builder()
            .namespaceName(namespaceName)
            .bucketName(bucketName)
            .objectName(key.value())
            .uploadId(uploadId)
            .build();
        client.abortMultipartUpload(request);
    }

    /**
     * Attempts to abort the multipart upload, suppressing any exception that occurs during abort
     * to preserve the original exception that triggered the abort.
     */
    private void tryAbortUpload(final Throwable originalException) {
        try {
            abortUpload();
        } catch (final RuntimeException abortEx) {
            log.error("Failed to abort multipart upload {}", uploadId, abortEx);
            originalException.addSuppressed(abortEx);
        }
    }

    private void flushBuffer(final ByteBuffer buffer,
                             final int actualPartSize,
                             final boolean multiPartUpload) {
        try (InputStream in = new ByteBufferInputStream(buffer)) {
            processedBytes += actualPartSize;
            if (multiPartUpload) {
                uploadPart(in, actualPartSize);
            } else {
                uploadAsSingleFile(in, actualPartSize);
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void uploadPart(final InputStream in, final int actualPartSize) {
        final int partNumber = completedParts.size() + 1;
        final UploadPartRequest uploadPartRequest = UploadPartRequest.builder()
            .namespaceName(namespaceName)
            .bucketName(bucketName)
            .objectName(key.value())
            .uploadId(uploadId)
            .uploadPartNum(partNumber)
            .contentLength((long) actualPartSize)
            .uploadPartBody(in)
            .build();
        final UploadPartResponse response = client.uploadPart(uploadPartRequest);
        final CommitMultipartUploadPartDetails completedPart = CommitMultipartUploadPartDetails.builder()
            .partNum(partNumber)
            .etag(response.getETag())
            .build();
        completedParts.add(completedPart);
    }

    private boolean storageTierDefined() {
        return storageTier != null && storageTier != StorageTier.UnknownEnumValue;
    }

    long processedBytes() {
        return processedBytes;
    }
}
