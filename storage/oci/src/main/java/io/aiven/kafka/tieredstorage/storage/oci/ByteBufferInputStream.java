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

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * An {@link InputStream} backed by a {@link ByteBuffer} that supports mark and reset.
 */
class ByteBufferInputStream extends InputStream {

    private final ByteBuffer buffer;

    ByteBufferInputStream(final ByteBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public int read() {
        if (!buffer.hasRemaining()) {
            return -1;
        }
        return buffer.get() & 0xFF;
    }

    @Override
    public int read(final byte[] b, final int off, final int len) {
        if (!buffer.hasRemaining()) {
            return -1;
        }
        final int toRead = Math.min(len, buffer.remaining());
        buffer.get(b, off, toRead);
        return toRead;
    }

    @Override
    public int available() {
        return buffer.remaining();
    }

    @Override
    public boolean markSupported() {
        return true;
    }

    @Override
    public synchronized void mark(final int readlimit) {
        buffer.mark();
    }

    @Override
    public synchronized void reset() {
        buffer.reset();
    }
}
