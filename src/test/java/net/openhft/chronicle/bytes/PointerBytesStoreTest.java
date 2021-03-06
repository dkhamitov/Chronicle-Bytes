/*
 * Copyright 2016-2020 chronicle.software
 *
 * https://chronicle.software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package net.openhft.chronicle.bytes;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class PointerBytesStoreTest extends BytesTestCommon {

    @Test
    public void testWrap() {
        final NativeBytesStore<Void> nbs = NativeBytesStore.nativeStore(10000);
        final PointerBytesStore pbs = BytesStore.nativePointer();
        try {
            pbs.set(nbs.addressForRead(nbs.start()), nbs.realCapacity());
            final long nanoTime = System.nanoTime();
            pbs.writeLong(0L, nanoTime);

            assertEquals(nanoTime, nbs.readLong(0L));
        } finally {
            nbs.releaseLast();
            pbs.releaseLast();
        }
    }

    @Test
    public void testWriteLimit() {
        final PointerBytesStore pbs = new PointerBytesStore();
        final Bytes<Void> wrapper = pbs.bytesForRead();
        pbs.set(NoBytesStore.NO_PAGE, 200);
        wrapper.writeLimit(pbs.capacity());
    }
}