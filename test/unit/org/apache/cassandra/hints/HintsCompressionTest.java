/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.hints;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.io.compress.DeflateCompressor;
import org.apache.cassandra.io.compress.ICompressor;
import org.apache.cassandra.io.compress.LZ4Compressor;
import org.apache.cassandra.io.compress.SnappyCompressor;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.utils.UUIDGen;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class HintsCompressionTest
{
    private static final String KEYSPACE = "hints_compression_test";
    private static final String TABLE = "table";


    private static Mutation createMutation(int index, long timestamp)
    {
        CFMetaData table = Schema.instance.getCFMetaData(KEYSPACE, TABLE);
        return new RowUpdateBuilder(table, timestamp, bytes(index))
               .clustering(bytes(index))
               .add("val", bytes(index))
               .build();
    }

    private static Hint createHint(int idx, long baseTimestamp)
    {
        long timestamp = baseTimestamp + idx;
        return Hint.create(createMutation(idx, TimeUnit.MILLISECONDS.toMicros(timestamp)), timestamp);
    }

    @BeforeClass
    public static void defineSchema()
    {
        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), SchemaLoader.standardCFMD(KEYSPACE, TABLE));
    }

    private ImmutableMap<String, Object> params(Class<? extends ICompressor> compressorClass)
    {
        ImmutableMap<String, Object> compressionParams = ImmutableMap.<String, Object>builder()
                                                                     .put(ParameterizedClass.CLASS_NAME, compressorClass.getSimpleName())
                                                                     .build();
        return ImmutableMap.<String, Object>builder()
                           .put(HintsDescriptor.COMPRESSION, compressionParams)
                           .build();
    }

    public void multiFlushAndDeserializeTest(Class<? extends ICompressor> compressorClass) throws Exception
    {
        int hintNum = 0;
        int bufferSize = HintsWriteExecutor.WRITE_BUFFER_SIZE;
        List<Hint> hints = new LinkedList<>();

        UUID hostId = UUIDGen.getTimeUUID();
        long ts = System.currentTimeMillis();

        HintsDescriptor descriptor = new HintsDescriptor(hostId, ts, params(compressorClass));
        File dir = Files.createTempDir();
        try (HintsWriter writer = HintsWriter.create(dir, descriptor))
        {
            assert writer instanceof CompressedHintsWriter;

            ByteBuffer writeBuffer = ByteBuffer.allocateDirect(bufferSize);
            try (HintsWriter.Session session = writer.newSession(writeBuffer))
            {
                while (session.getBytesWritten() < bufferSize * 3)
                {
                    Hint hint = createHint(hintNum, ts+hintNum);
                    session.append(hint);
                    hints.add(hint);
                    hintNum++;
                }
            }
        }

        try (HintsReader reader = HintsReader.open(new File(dir, descriptor.fileName())))
        {
            List<Hint> deserialized = new ArrayList<>(hintNum);
            List<InputPosition> pagePositions = new ArrayList<>(hintNum);

            for (HintsReader.Page page: reader)
            {
                pagePositions.add(page.position);
                Iterator<Hint> iterator = page.hintsIterator();
                while (iterator.hasNext())
                {
                    deserialized.add(iterator.next());
                }
            }

            Assert.assertEquals(hints.size(), deserialized.size());
            hintNum = 0;
            for (Hint expected: hints)
            {
                HintsTestUtil.assertHintsEqual(expected, deserialized.get(hintNum));
                hintNum++;
            }

            // explicitely seek to each page by iterating collected page positions and check if hints still match as expected
            int hintOffset = 0;
            for (InputPosition pos : pagePositions)
            {
                reader.seek(pos);
                HintsReader.Page page = reader.iterator().next();
                Iterator<Hint> iterator = page.hintsIterator();
                while (iterator.hasNext())
                {
                    Hint seekedHint = iterator.next();
                    HintsTestUtil.assertHintsEqual(hints.get(hintOffset), seekedHint);
                    hintOffset++;
                }
            }
        }
    }

    @Test
    public void lz4Compressor() throws Exception
    {
        multiFlushAndDeserializeTest(LZ4Compressor.class);
    }

    @Test
    public void snappyCompressor() throws Exception
    {
        multiFlushAndDeserializeTest(SnappyCompressor.class);
    }

    @Test
    public void deflateCompressor() throws Exception
    {
        multiFlushAndDeserializeTest(DeflateCompressor.class);
    }
}
