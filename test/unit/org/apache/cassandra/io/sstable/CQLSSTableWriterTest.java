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
package org.apache.cassandra.io.sstable;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.OutputHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

public class CQLSSTableWriterTest
{
    @BeforeClass
    public static void setup() throws Exception
    {
        DatabaseDescriptor.setDaemonInitialized();
        SchemaLoader.cleanupAndLeaveDirs();
        Keyspace.setInitialized();
        StorageService.instance.initServer();
    }

    @AfterClass
    public static void tearDown() throws Exception
    {
        Config.setClientMode(false);
    }

    @Test
    public void testUnsortedWriter() throws Exception
    {
        try (AutoCloseable switcher = Util.switchPartitioner(ByteOrderedPartitioner.instance))
        {
            String KS = "cql_keyspace";
            String TABLE = "table1";

            File tempdir = Files.createTempDir();
            File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
            assert dataDir.mkdirs();

            String schema = "CREATE TABLE cql_keyspace.table1 ("
                          + "  k int PRIMARY KEY,"
                          + "  v1 text,"
                          + "  v2 int"
                          + ")";
            String insert = "INSERT INTO cql_keyspace.table1 (k, v1, v2) VALUES (?, ?, ?)";
            CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                      .inDirectory(dataDir)
                                                      .forTable(schema)
                                                      .using(insert).build();

            writer.addRow(0, "test1", 24);
            writer.addRow(1, "test2", 44);
            writer.addRow(2, "test3", 42);
            writer.addRow(ImmutableMap.<String, Object>of("k", 3, "v2", 12));

            writer.close();

            loadSSTables(dataDir, KS);

            UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace.table1;");
            assertEquals(4, rs.size());

            Iterator<UntypedResultSet.Row> iter = rs.iterator();
            UntypedResultSet.Row row;

            row = iter.next();
            assertEquals(0, row.getInt("k"));
            assertEquals("test1", row.getString("v1"));
            assertEquals(24, row.getInt("v2"));

            row = iter.next();
            assertEquals(1, row.getInt("k"));
            assertEquals("test2", row.getString("v1"));
            //assertFalse(row.has("v2"));
            assertEquals(44, row.getInt("v2"));

            row = iter.next();
            assertEquals(2, row.getInt("k"));
            assertEquals("test3", row.getString("v1"));
            assertEquals(42, row.getInt("v2"));

            row = iter.next();
            assertEquals(3, row.getInt("k"));
            assertEquals(null, row.getBytes("v1")); // Using getBytes because we know it won't NPE
            assertEquals(12, row.getInt("v2"));
        }
    }

    @Test
    public void testForbidCounterUpdates() throws Exception
    {
        String KS = "cql_keyspace";
        String TABLE = "counter1";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        String schema = "CREATE TABLE cql_keyspace.counter1 (" +
                        "  my_id int, " +
                        "  my_counter counter, " +
                        "  PRIMARY KEY (my_id)" +
                        ")";
        String insert = String.format("UPDATE cql_keyspace.counter1 SET my_counter = my_counter - ? WHERE my_id = ?");
        try
        {
            CQLSSTableWriter.builder().inDirectory(dataDir)
                            .forTable(schema)
                            .withPartitioner(Murmur3Partitioner.instance)
                            .using(insert).build();
            fail("Counter update statements should not be supported");
        }
        catch (IllegalArgumentException e)
        {
            assertEquals(e.getMessage(), "Counter update statements are not supported");
        }
    }

    @Test
    public void testSyncWithinPartition() throws Exception
    {
        // Check that the write respect the buffer size even if we only insert rows withing the same partition (#7360)
        // To do that simply, we use a writer with a buffer of 1MB, and write 2 rows in the same partition with a value
        // > 1MB and validate that this created more than 1 sstable.
        String KS = "ks";
        String TABLE = "test";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();
        String schema = "CREATE TABLE ks.test ("
                      + "  k int PRIMARY KEY,"
                      + "  v blob"
                      + ")";
        String insert = "INSERT INTO ks.test (k, v) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();

        ByteBuffer val = ByteBuffer.allocate(1024 * 1050);

        writer.addRow(0, val);
        writer.addRow(1, val);
        writer.close();

        FilenameFilter filterDataFiles = new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                return name.endsWith("-Data.db");
            }
        };
        assert dataDir.list(filterDataFiles).length > 1 : Arrays.toString(dataDir.list(filterDataFiles));
    }


    @Test
    public void testSyncNoEmptyRows() throws Exception
    {
        // Check that the write does not throw an empty partition error (#9071)
        File tempdir = Files.createTempDir();
        String schema = "CREATE TABLE ks.test2 ("
                        + "  k UUID,"
                        + "  c int,"
                        + "  PRIMARY KEY (k)"
                        + ")";
        String insert = "INSERT INTO ks.test2 (k, c) VALUES (?, ?)";
        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(tempdir)
                                                  .forTable(schema)
                                                  .using(insert)
                                                  .withBufferSizeInMB(1)
                                                  .build();

        for (int i = 0 ; i < 50000 ; i++) {
            writer.addRow(UUID.randomUUID(), 0);
        }
        writer.close();

    }

    @Test
    public void testUpdateStatement() throws Exception
    {
        final String KS = "cql_keyspace6";
        final String TABLE = "table6";

        final String schema = "CREATE TABLE " + KS + "." + TABLE + " ("
                              + "  k int,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v text,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("UPDATE " + KS + "." + TABLE + " SET v = ? " +
                                                         "WHERE k = ? AND c1 = ? AND c2 = ?")
                                                  .build();

        writer.addRow("a", 1, 2, 3);
        writer.addRow("b", 4, 5, 6);
        writer.addRow(null, 7, 8, 9);
        writer.close();
        loadSSTables(dataDir, KS);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + KS + "." + TABLE);
        assertEquals(2, resultSet.size());

        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals(1, r1.getInt("k"));
        assertEquals(2, r1.getInt("c1"));
        assertEquals(3, r1.getInt("c2"));
        assertEquals("a", r1.getString("v"));
        UntypedResultSet.Row r2 = iter.next();
        assertEquals(4, r2.getInt("k"));
        assertEquals(5, r2.getInt("c1"));
        assertEquals(6, r2.getInt("c2"));
        assertEquals("b", r2.getString("v"));
        assertFalse(iter.hasNext());
    }

    @Test
    public void testNativeFunctions() throws Exception
    {
        final String KS = "cql_keyspace7";
        final String TABLE = "table7";

        final String schema = "CREATE TABLE " + KS + "." + TABLE + " ("
                              + "  k int,"
                              + "  c1 int,"
                              + "  c2 int,"
                              + "  v blob,"
                              + "  PRIMARY KEY (k, c1, c2)"
                              + ")";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        CQLSSTableWriter writer = CQLSSTableWriter.builder()
                                                  .inDirectory(dataDir)
                                                  .forTable(schema)
                                                  .using("INSERT INTO " + KS + "." + TABLE + " (k, c1, c2, v) VALUES (?, ?, ?, textAsBlob(?))")
                                                  .build();

        writer.addRow(1, 2, 3, "abc");
        writer.addRow(4, 5, 6, "efg");

        writer.close();
        loadSSTables(dataDir, KS);

        UntypedResultSet resultSet = QueryProcessor.executeInternal("SELECT * FROM " + KS + "." + TABLE);
        assertEquals(2, resultSet.size());

        Iterator<UntypedResultSet.Row> iter = resultSet.iterator();
        UntypedResultSet.Row r1 = iter.next();
        assertEquals(1, r1.getInt("k"));
        assertEquals(2, r1.getInt("c1"));
        assertEquals(3, r1.getInt("c2"));
        assertEquals(ByteBufferUtil.bytes("abc"), r1.getBytes("v"));

        UntypedResultSet.Row r2 = iter.next();
        assertEquals(4, r2.getInt("k"));
        assertEquals(5, r2.getInt("c1"));
        assertEquals(6, r2.getInt("c2"));
        assertEquals(ByteBufferUtil.bytes("efg"), r2.getBytes("v"));

        assertFalse(iter.hasNext());
    }

    private static final int NUMBER_WRITES_IN_RUNNABLE = 10;
    private class WriterThread extends Thread
    {
        private final File dataDir;
        private final int id;
        public volatile Exception exception;

        public WriterThread(File dataDir, int id)
        {
            this.dataDir = dataDir;
            this.id = id;
        }

        @Override
        public void run()
        {
            String schema = "CREATE TABLE cql_keyspace2.table2 ("
                    + "  k int,"
                    + "  v int,"
                    + "  PRIMARY KEY (k, v)"
                    + ")";
            String insert = "INSERT INTO cql_keyspace2.table2 (k, v) VALUES (?, ?)";
            CQLSSTableWriter writer = CQLSSTableWriter.builder()
                    .inDirectory(dataDir)
                    .forTable(schema)
                    .using(insert).build();

            try
            {
                for (int i = 0; i < NUMBER_WRITES_IN_RUNNABLE; i++)
                {
                    writer.addRow(id, i);
                }
                writer.close();
            }
            catch (Exception e)
            {
                exception = e;
            }
        }
    }

    @Test
    public void testConcurrentWriters() throws Exception
    {
        final String KS = "cql_keyspace2";
        final String TABLE = "table2";

        File tempdir = Files.createTempDir();
        File dataDir = new File(tempdir.getAbsolutePath() + File.separator + KS + File.separator + TABLE);
        assert dataDir.mkdirs();

        WriterThread[] threads = new WriterThread[5];
        for (int i = 0; i < threads.length; i++)
        {
            WriterThread thread = new WriterThread(dataDir, i);
            threads[i] = thread;
            thread.start();
        }

        for (WriterThread thread : threads)
        {
            thread.join();
            assert !thread.isAlive() : "Thread should be dead by now";
            if (thread.exception != null)
            {
                throw thread.exception;
            }
        }

        loadSSTables(dataDir, KS);

        UntypedResultSet rs = QueryProcessor.executeInternal("SELECT * FROM cql_keyspace2.table2;");
        assertEquals(threads.length * NUMBER_WRITES_IN_RUNNABLE, rs.size());
    }

    private static void loadSSTables(File dataDir, String ks) throws ExecutionException, InterruptedException
    {
        SSTableLoader loader = new SSTableLoader(dataDir, new SSTableLoader.Client()
        {
            private String keyspace;

            public void init(String keyspace)
            {
                this.keyspace = keyspace;
                for (Range<Token> range : StorageService.instance.getLocalRanges(ks))
                    addRangeForEndpoint(range, FBUtilities.getBroadcastAddress());
            }

            public CFMetaData getTableMetadata(String cfName)
            {
                return Schema.instance.getCFMetaData(keyspace, cfName);
            }
        }, new OutputHandler.SystemOutput(false, false));

        loader.stream().get();
    }
}
