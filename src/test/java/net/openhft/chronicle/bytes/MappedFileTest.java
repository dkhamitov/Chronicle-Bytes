/*
 * Copyright 2016 higherfrequencytrading.com
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

import net.openhft.chronicle.core.OS;
import net.openhft.chronicle.core.ReferenceCounted;
import net.openhft.chronicle.core.ReferenceOwner;
import net.openhft.chronicle.core.threads.ThreadDump;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.*;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;

import static org.junit.Assert.*;
import static org.junit.Assume.assumeTrue;

public class MappedFileTest {
    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();
    private ThreadDump threadDump;

    @After
    public void checkRegisteredBytes() {
        BytesUtil.checkRegisteredBytes();
    }

    @Before
    public void threadDump() {
        threadDump = new ThreadDump();
    }

    @After
    public void checkThreadDump() {
        threadDump.assertNoNewThreads();
    }

    @Test
    public void testWarmup() throws InterruptedException {
        MappedFile.warmup();
    }

    @Test
    public void shouldReleaseReferenceWhenNewStoreIsAcquired() throws IOException {
        final File file = tmpDir.newFile();
        // this is what it will end up as
        final long chunkSize = OS.mapAlign(64);
        final MappedFile mappedFile = MappedFile.mappedFile(file, 64);
        final MappedBytesStore first = mappedFile.acquireByteStore(1);
        ReferenceOwner temp = ReferenceOwner.temporary("test");
        first.reserve(temp);

        assertEquals(2, first.refCount());
        assertEquals(2, mappedFile.refCount());

        final MappedBytesStore second = mappedFile.acquireByteStore(1 + chunkSize);
        second.reserve(temp);

        assertEquals(2, first.refCount());
        assertEquals(2, second.refCount());
        assertEquals(3, mappedFile.refCount());

        final MappedBytesStore third = mappedFile.acquireByteStore(1 + chunkSize + chunkSize);
        third.reserve(temp);

        assertEquals(2, first.refCount());
        assertEquals(2, second.refCount());
        assertEquals(2, third.refCount());
        assertEquals(4, mappedFile.refCount());
        for (ReferenceCounted rc : new ReferenceCounted[]{first, second, third}) {
            rc.release(temp);
        }
        mappedFile.releaseLast();
    }

    @SuppressWarnings("rawtypes")
    @Test
    public void testReferenceCounts() throws IOException {
        new File(OS.TARGET).mkdir();
        @NotNull File tmp = new File(OS.TARGET, "testReferenceCounts-" + System.nanoTime() + ".bin");
        tmp.deleteOnExit();
        int chunkSize = OS.isWindows() ? 64 << 10 : 4 << 10;
        @NotNull MappedFile mf = MappedFile.mappedFile(tmp, chunkSize, 0);
        assertEquals("refCount: 1", mf.referenceCounts());

        ReferenceOwner temp = ReferenceOwner.temporary("temp");

        @Nullable MappedBytesStore bs = mf.acquireByteStore(chunkSize + (1 << 10));
        bs.reserve(temp);

        assertEquals(chunkSize, bs.start());
        assertEquals(chunkSize * 2, bs.capacity());
        Bytes bytes = bs.bytesForRead();

        assertNotNull(bytes.toString()); // show it doesn't blow up.
        assertNotNull(bs.toString()); // show it doesn't blow up.
        assertEquals(chunkSize, bytes.start());
        assertEquals(0L, bs.readLong(chunkSize + (1 << 10)));
        assertEquals(0L, bytes.readLong(chunkSize + (1 << 10)));
        Assert.assertFalse(bs.inside(chunkSize - (1 << 10)));
        Assert.assertFalse(bs.inside(chunkSize - 1));
        assertTrue(bs.inside(chunkSize));
        assertTrue(bs.inside(chunkSize * 2 - 1));
        Assert.assertFalse(bs.inside(chunkSize * 2));
        try {
            bytes.readLong(chunkSize - (1 << 10));
            Assert.fail();
        } catch (BufferUnderflowException e) {
            // expected
        }
        try {
            bytes.readLong(chunkSize * 2 + (1 << 10));
            Assert.fail();
        } catch (BufferUnderflowException e) {
            // expected
        }
        assertEquals(2, mf.refCount());
        assertEquals(3, bs.refCount());
        assertEquals("refCount: 2, 0, 3", mf.referenceCounts());

        ReferenceOwner temp2 = ReferenceOwner.temporary("temp2");
        @Nullable BytesStore bs2 = mf.acquireByteStore(chunkSize + (1 << 10));
        bs2.reserve(temp2);

        assertEquals(4, bs2.refCount());
        assertEquals("refCount: 2, 0, 4", mf.referenceCounts());
        bytes.releaseLast();

        assertEquals(3, bs2.refCount());
        assertEquals("refCount: 2, 0, 3", mf.referenceCounts());

        bs2.release(temp2);
        assert bs == bs2;
        // bs2.releaseLast(); as bs and bs2 are the same.
        bs.release(temp);
        assertEquals("refCount: 1, 0, 0", mf.referenceCounts());

        mf.close();
        assertEquals(0, bs2.refCount());
        assertEquals(0, bs.refCount());
        assertEquals(0, mf.refCount());
        assertEquals("refCount: 0, 0, 0", mf.referenceCounts());
    }

    @Test
    public void largeReadOnlyFile() throws IOException {
        assumeTrue(OS.isLinux());

        @NotNull File file = File.createTempFile("largeReadOnlyFile", "deleteme");
        file.deleteOnExit();
        try (@NotNull MappedBytes bytes = MappedBytes.mappedBytes(file, 1 << 30, OS.pageSize())) {
            bytes.writeLong(3L << 30, 0x12345678); // make the file 3 GB.
        }

        try (@NotNull MappedBytes bytes = MappedBytes.readOnly(file)) {
            assertEquals(0x12345678L, bytes.readLong(3L << 30));
        }
    }

    @Test
    public void interrupted() throws FileNotFoundException {
        Thread.currentThread().interrupt();
        String filename = OS.TARGET + "/interrupted-" + System.nanoTime();
        new File(filename).deleteOnExit();
        MappedFile mf = MappedFile.mappedFile(filename, 64 << 10, 0);
        try {
            mf.actualSize();
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            mf.releaseLast();
        }
    }

    @Test
    public void testCreateMappedFile() throws IOException {
        File file = File.createTempFile("mappedFile", "");
        file.deleteOnExit();

        MappedFile mappedFile = MappedFile.mappedFile(file, 1024, 256, 256, false);
        MappedFile mappedFile2 = MappedFile.mappedFile(file, 1024, 256, 256, false);

        mappedFile.releaseLast();
        mappedFile2.releaseLast();
    }

    @After
    public void clearInterrupt() {
        Thread.interrupted();
    }
}