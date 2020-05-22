/*
 * Copyright 2016 higherfrequencytrading.com
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package net.openhft.chronicle.bytes;

import net.openhft.chronicle.bytes.algo.BytesStoreHash;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.core.ReferenceCounter;
import net.openhft.chronicle.core.ReferenceOwner;

import java.nio.BufferUnderflowException;

/*
 * Created by Peter Lawrey on 07/05/16.
 */
public abstract class AbstractBytesStore<B extends BytesStore<B, Underlying>, Underlying>
        implements BytesStore<B, Underlying> {
    protected final ReferenceCounted referenceCounted = ReferenceCounter.onReleased(this::performRelease, this.releaseOnOne());

    protected boolean releaseOnOne() {
        return false;
    }

    @Override
    public void reserve(ReferenceOwner id) throws IllegalStateException {
        referenceCounted.reserve(id);
    }

    @Override
    public void release(ReferenceOwner id) throws IllegalStateException {
        referenceCounted.release(id);
    }

    @Override
    public int refCount() {
        return referenceCounted.refCount();
    }

    @Override
    public boolean tryReserve(ReferenceOwner id) {
        return referenceCounted.tryReserve(id);
    }

    @Override
    public void releaseLast(ReferenceOwner id) {
        referenceCounted.releaseLast(id);
    }

    protected abstract void performRelease();

    @Override
    public int peekUnsignedByte(long offset) throws BufferUnderflowException {
        return offset >= readLimit() ? -1 : readUnsignedByte(offset);
    }

    @Override
    public int hashCode() {
        return BytesStoreHash.hash32(this);
    }

    @Override
    public long readPosition() {
        return 0L;
    }

    @Override
    public long readRemaining() {
        return this.capacity();
    }

    @Override
    public long start() {
        return 0L;
    }
}
