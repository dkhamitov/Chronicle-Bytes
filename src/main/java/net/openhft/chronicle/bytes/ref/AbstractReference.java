package net.openhft.chronicle.bytes.ref;

import net.openhft.chronicle.bytes.Byteable;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.core.ReferenceOwner;
import net.openhft.chronicle.core.StackTrace;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

@SuppressWarnings("rawtypes")
public abstract class AbstractReference implements Byteable, Closeable, ReferenceOwner {

    @Nullable
    protected BytesStore bytes;
    protected long offset;
    private StackTrace closedHere;

    @Override
    public void bytesStore(@NotNull final BytesStore bytes, final long offset, final long length) throws IllegalStateException, IllegalArgumentException, BufferOverflowException, BufferUnderflowException {
        acceptNewBytesStore(bytes);
        this.offset = offset;
    }

    @Nullable
    @Override
    public BytesStore bytesStore() {
        return bytes;
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public abstract long maxSize();

    protected void acceptNewBytesStore(@NotNull final BytesStore bytes) {
        if (this.bytes != null) {
            this.bytes.release(this);
        }
        this.bytes = bytes.bytesStore();
        this.bytes.reserve(this);
    }

    @Override
    public void close() {
        if (this.bytes != null) {
            this.bytes.release(this);
            this.bytes = null;
            closedHere = new StackTrace("closed here");
        }
    }

    public long address() {
        return bytesStore().addressForRead(offset);
    }
}
