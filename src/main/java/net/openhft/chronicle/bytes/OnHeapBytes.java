package net.openhft.chronicle.bytes;

import net.openhft.chronicle.bytes.util.DecoratedBufferOverflowException;
import net.openhft.chronicle.core.Jvm;
import org.jetbrains.annotations.NotNull;

import java.nio.BufferOverflowException;

public class OnHeapBytes extends VanillaBytes<byte[]> {
    public static final int MAX_CAPACITY = Bytes.MAX_HEAP_CAPACITY;
    private final boolean elastic;
    private final long capacity;

    public OnHeapBytes(@NotNull BytesStore bytesStore, boolean elastic)
            throws IllegalStateException, IllegalArgumentException {
        super(bytesStore);
        this.elastic = elastic;
        this.capacity = elastic ? MAX_CAPACITY : bytesStore.capacity();

        try {
            writePosition(0);
            writeLimit(capacity());
        } catch (BufferOverflowException e) {
            throw new AssertionError(e);
        }
    }

    @Override
    public long capacity() {
        return capacity;
    }

    @Override
    public void ensureCapacity(long desiredCapacity) throws IllegalArgumentException, IllegalStateException {
        if (isElastic() && bytesStore.capacity() < desiredCapacity)
            resize(desiredCapacity);
        else
            super.ensureCapacity(desiredCapacity);
    }

    @Override
    public boolean isElastic() {
        return elastic;
    }

    @Override
    protected void writeCheckOffset(long offset, long adding)
            throws BufferOverflowException, IllegalStateException {
        if (offset >= bytesStore.start()) {
            long writeEnd = offset + adding;
            if (writeEnd > writeLimit)
                throwBeyondWriteLimit(adding, writeEnd);
            if (writeEnd <= bytesStore.safeLimit()) {
                return; // do nothing.
            }
            checkResize(writeEnd);
        } else {
            throw new BufferOverflowException();
        }
    }

    private void throwBeyondWriteLimit(long advance, long writeEnd)
            throws DecoratedBufferOverflowException {
        throw new DecoratedBufferOverflowException("attempt to write " + advance + " bytes to " + writeEnd + " limit: " + writeLimit);
    }

    private void checkResize(long endOfBuffer)
            throws BufferOverflowException, IllegalStateException {
        if (isElastic())
            resize(endOfBuffer);
        else
            throw new BufferOverflowException();
    }

    // the endOfBuffer is the minimum capacity and one byte more than the last addressable byte.
    private void resize(long endOfBuffer)
            throws BufferOverflowException, IllegalStateException {
        if (endOfBuffer < 0)
            throw new BufferOverflowException();
        if (endOfBuffer > capacity())
            throw new BufferOverflowException();
        final long realCapacity = realCapacity();
        if (endOfBuffer <= realCapacity) {
//            System.out.println("no resize " + endOfBuffer + " < " + realCapacity);
            return;
        }

        // Grow by 50%
        long size0 = Math.max(endOfBuffer, realCapacity * 3 / 2);
        // Size must not be more than capacity(), it may break some assumptions in BytesStore or elsewhere
        int size = (int) Math.min(size0, capacity());

        //        System.out.println("resize " + endOfBuffer + " to " + size);
        // native block of 128 KiB or more have an individual memory mapping so are more expensive.
        if (endOfBuffer >= 128 << 10)
            Jvm.perf().on(getClass(), "Resizing buffer was " + realCapacity / 1024 + " KB, " +
                    "needs " + (endOfBuffer - realCapacity) + " bytes more, " +
                    "new-size " + size / 1024 + " KB");
        HeapBytesStore store;
        try {
            store = HeapBytesStore.wrap(new byte[size]);
            store.reserveTransfer(INIT, this);
        } catch (IllegalStateException e) {
            BufferOverflowException boe = new BufferOverflowException();
            boe.initCause(e);
            throw boe;
        }

        BytesStore<Bytes<byte[]>, byte[]> tempStore = this.bytesStore;
        this.bytesStore.copyTo(store);
        this.bytesStore(store);
        try {
            tempStore.release(this);
        } catch (IllegalStateException e) {
            Jvm.debug().on(getClass(), e);
        }
    }
}
