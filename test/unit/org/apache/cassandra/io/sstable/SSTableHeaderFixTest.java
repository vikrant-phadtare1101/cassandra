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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.SerializationHeader;
import org.apache.cassandra.db.marshal.AbstractCompositeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.FrozenType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.rows.EncodingStats;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.format.big.BigFormat;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.SequentialWriter;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test the functionality of {@link SSTableHeaderFix}.
 * It writes an 'big-m' version sstable(s) and executes against these.
 */
public class SSTableHeaderFixTest
{
    static
    {
        DatabaseDescriptor.toolInitialization();
    }

    private File temporaryFolder;

    @Before
    public void setup()
    {
        File f = FileUtils.createTempFile("SSTableUDTFixTest", "");
        f.delete();
        f.mkdirs();
        temporaryFolder = f;
    }

    @After
    public void teardown()
    {
        FileUtils.deleteRecursive(temporaryFolder);
    }

    private static final AbstractType<?> udtPK = makeUDT("udt_pk");
    private static final AbstractType<?> udtCK = makeUDT("udt_ck");
    private static final AbstractType<?> udtStatic = makeUDT("udt_static");
    private static final AbstractType<?> udtRegular = makeUDT("udt_regular");
    private static final AbstractType<?> udtInner = makeUDT("udt_inner");
    private static final AbstractType<?> udtNested = new UserType("ks",
                                                                  ByteBufferUtil.bytes("udt_nested"),
                                                                  Arrays.asList(new FieldIdentifier(ByteBufferUtil.bytes("a_field")),
                                                                                new FieldIdentifier(ByteBufferUtil.bytes("a_udt"))),
                                                                  Arrays.asList(UTF8Type.instance,
                                                                                udtInner),
                                                                  true);
    private static final AbstractType<?> tupleInTuple = makeTuple(makeTuple());
    private static final AbstractType<?> udtInTuple = makeTuple(udtInner);
    private static final AbstractType<?> tupleInComposite = CompositeType.getInstance(UTF8Type.instance, makeTuple());
    private static final AbstractType<?> udtInComposite = CompositeType.getInstance(UTF8Type.instance, udtInner);
    private static final AbstractType<?> udtInList = ListType.getInstance(udtInner, true);
    private static final AbstractType<?> udtInSet = SetType.getInstance(udtInner, true);
    private static final AbstractType<?> udtInMap = MapType.getInstance(UTF8Type.instance, udtInner, true);
    private static final AbstractType<?> udtInFrozenList = ListType.getInstance(udtInner, false);
    private static final AbstractType<?> udtInFrozenSet = SetType.getInstance(udtInner, false);
    private static final AbstractType<?> udtInFrozenMap = MapType.getInstance(UTF8Type.instance, udtInner, false);

    private static AbstractType<?> makeUDT(String udtName)
    {
        return new UserType("ks",
                            ByteBufferUtil.bytes(udtName),
                            Collections.singletonList(new FieldIdentifier(ByteBufferUtil.bytes("a_field"))),
                            Collections.singletonList(UTF8Type.instance),
                            true);
    }

    private static TupleType makeTuple()
    {
        return makeTuple(Int32Type.instance);
    }

    private static TupleType makeTuple(AbstractType<?> second)
    {
        return new TupleType(Arrays.asList(UTF8Type.instance,
                                           second));
    }

    private static final Version version = BigFormat.instance.getVersion("mc");

    private TableMetadata tableMetadata;
    private final Set<String> updatedColumns = new HashSet<>();

    /**
     * Very basic test whether {@link SSTableHeaderFix} detect a type mismatch (regular_c 'int' vs 'float').
     */
    @Test
    public void verifyTypeMismatchTest() throws Exception
    {
        File dir = temporaryFolder;
        File sstable = generateFakeSSTable(dir, 1);

        SerializationHeader.Component header = readHeader(sstable);
        assertFrozenUdt(header, false);

        ColumnMetadata cd = tableMetadata.getColumn(ByteBufferUtil.bytes("regular_c"));
        tableMetadata = tableMetadata.unbuild()
                                     .removeRegularOrStaticColumn(cd.name)
                                     .addRegularColumn("regular_c", FloatType.instance)
                                     .build();

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertTrue(headerFix.hasError());
        assertTrue(headerFix.hasChanges());

        // must not have re-written the stats-component
        header = readHeader(sstable);
        assertFrozenUdt(header, false);
    }

    @Test
    public void verifyTypeMatchTest() throws Exception
    {
        File dir = temporaryFolder;

        TableMetadata.Builder cols = TableMetadata.builder("ks", "cf")
                                                  .addPartitionKeyColumn("pk", udtPK)
                                                  .addClusteringColumn("ck", udtCK);
        commonColumns(cols);
        File sstable = buildFakeSSTable(dir, 1, cols, false);

        SerializationHeader.Component header = readHeader(sstable);
        assertFrozenUdt(header, false);

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertTrue(updatedColumns.isEmpty());
        assertFalse(headerFix.hasError());
        assertFalse(headerFix.hasChanges());

        // must not have re-written the stats-component
        header = readHeader(sstable);
        assertFrozenUdt(header, false);
    }

    /**
     * Simulates the case when an sstable contains a column not present in the schema, which can just be ignored.
     */
    @Test
    public void verifyWithUnknownColumnTest() throws Exception
    {
        File dir = temporaryFolder;
        TableMetadata.Builder cols = TableMetadata.builder("ks", "cf")
                                                  .addPartitionKeyColumn("pk", udtPK)
                                                  .addClusteringColumn("ck", udtCK);
        commonColumns(cols);
        cols.addRegularColumn("solr_query", UTF8Type.instance);
        File sstable = buildFakeSSTable(dir, 1, cols, true);

        SerializationHeader.Component header = readHeader(sstable);
        assertFrozenUdt(header, false);

        ColumnMetadata cd = tableMetadata.getColumn(ByteBufferUtil.bytes("solr_query"));
        tableMetadata = tableMetadata.unbuild()
                                     .removeRegularOrStaticColumn(cd.name)
                                     .build();

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertFalse(headerFix.hasError());
        assertTrue(headerFix.hasChanges());

        // must not have re-written the stats-component
        header = readHeader(sstable);
        assertFrozenUdt(header, true);
    }

    /**
     * Simulates the case when an sstable contains a column not present in the table but as a target for an index.
     * It can just be ignored.
     */
    @Test
    public void verifyWithIndexedUnknownColumnTest() throws Exception
    {
        File dir = temporaryFolder;
        TableMetadata.Builder cols = TableMetadata.builder("ks", "cf")
                                                  .addPartitionKeyColumn("pk", udtPK)
                                                  .addClusteringColumn("ck", udtCK);
        commonColumns(cols);
        cols.addRegularColumn("solr_query", UTF8Type.instance);
        File sstable = buildFakeSSTable(dir, 1, cols, true);

        SerializationHeader.Component header = readHeader(sstable);
        assertFrozenUdt(header, false);

        ColumnMetadata cd = tableMetadata.getColumn(ByteBufferUtil.bytes("solr_query"));
        tableMetadata = tableMetadata.unbuild()
                                     .indexes(tableMetadata.indexes.with(IndexMetadata.fromSchemaMetadata("some search index", IndexMetadata.Kind.CUSTOM, Collections.singletonMap(IndexTarget.TARGET_OPTION_NAME, "solr_query"))))
                                     .removeRegularOrStaticColumn(cd.name)
                                     .build();

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertFalse(headerFix.hasError());
        assertTrue(headerFix.hasChanges());

        // must not have re-written the stats-component
        header = readHeader(sstable);
        assertFrozenUdt(header, true);
    }

    @Test
    public void complexTypeMatchTest() throws Exception
    {
        File dir = temporaryFolder;

        TableMetadata.Builder cols = TableMetadata.builder("ks", "cf")
                                                  .addPartitionKeyColumn("pk", udtPK)
                                                  .addClusteringColumn("ck", udtCK);
        commonColumns(cols);
        cols.addRegularColumn("tuple_in_tuple", tupleInTuple)
            .addRegularColumn("udt_nested", udtNested)
            .addRegularColumn("udt_in_tuple", udtInTuple)
            .addRegularColumn("tuple_in_composite", tupleInComposite)
            .addRegularColumn("udt_in_composite", udtInComposite)
            .addRegularColumn("udt_in_list", udtInList)
            .addRegularColumn("udt_in_set", udtInSet)
            .addRegularColumn("udt_in_map", udtInMap)
            .addRegularColumn("udt_in_frozen_list", udtInFrozenList)
            .addRegularColumn("udt_in_frozen_set", udtInFrozenSet)
            .addRegularColumn("udt_in_frozen_map", udtInFrozenMap);
        File sstable = buildFakeSSTable(dir, 1, cols, true);

        SerializationHeader.Component header = readHeader(sstable);
        assertFrozenUdt(header, false);

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertFalse(headerFix.hasError());
        assertTrue(headerFix.hasChanges());
        assertEquals(Sets.newHashSet("pk", "ck", "regular_b", "static_b",
                                     "udt_nested", "udt_in_composite", "udt_in_list", "udt_in_set", "udt_in_map"), updatedColumns);

        // must not have re-written the stats-component
        header = readHeader(sstable);
        assertFrozenUdt(header, true);
    }

    @Test
    public void complexTypeDroppedColumnsMatchTest() throws Exception
    {
        File dir = temporaryFolder;

        TableMetadata.Builder cols = TableMetadata.builder("ks", "cf")
                                                  .addPartitionKeyColumn("pk", udtPK)
                                                  .addClusteringColumn("ck", udtCK);
        commonColumns(cols);
        cols.addRegularColumn("tuple_in_tuple", tupleInTuple)
            .addRegularColumn("udt_nested", udtNested)
            .addRegularColumn("udt_in_tuple", udtInTuple)
            .addRegularColumn("tuple_in_composite", tupleInComposite)
            .addRegularColumn("udt_in_composite", udtInComposite)
            .addRegularColumn("udt_in_list", udtInList)
            .addRegularColumn("udt_in_set", udtInSet)
            .addRegularColumn("udt_in_map", udtInMap)
            .addRegularColumn("udt_in_frozen_list", udtInFrozenList)
            .addRegularColumn("udt_in_frozen_set", udtInFrozenSet)
            .addRegularColumn("udt_in_frozen_map", udtInFrozenMap);
        File sstable = buildFakeSSTable(dir, 1, cols, true);

        cols = tableMetadata.unbuild();
        for (String col : new String[]{"tuple_in_tuple", "udt_nested", "udt_in_tuple",
                                       "tuple_in_composite", "udt_in_composite",
                                       "udt_in_list", "udt_in_set", "udt_in_map",
                                       "udt_in_frozen_list", "udt_in_frozen_set", "udt_in_frozen_map"})
        {
            ColumnIdentifier ci = new ColumnIdentifier(col, true);
            ColumnMetadata cd = tableMetadata.getColumn(ci);
            cols.removeRegularOrStaticColumn(ci)
                .recordColumnDrop(new ColumnMetadata(cd.ksName, cd.cfName, cd.name, Int32Type.instance, cd.position(), cd.kind), FBUtilities.timestampMicros());
        }
        tableMetadata = cols.build();

        SerializationHeader.Component header = readHeader(sstable);
        assertFrozenUdt(header, false);

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertFalse(headerFix.hasError());
        assertTrue(headerFix.hasChanges());
        assertEquals(Sets.newHashSet("pk", "ck", "regular_b", "static_b"), updatedColumns);

        // must not have re-written the stats-component
        header = readHeader(sstable);
        // assertFrozenUdt(header, true); // note: cannot assert here, the columns have been dropped, so those are left as they are
    }

    @Test
    public void verifyTypeMatchCompositeKeyTest() throws Exception
    {
        File dir = temporaryFolder;

        TableMetadata.Builder cols = TableMetadata.builder("ks", "cf")
                                                  .addPartitionKeyColumn("pk1", UTF8Type.instance)
                                                  .addPartitionKeyColumn("pk2", udtPK)
                                                  .addClusteringColumn("ck", udtCK);
        commonColumns(cols);
        File sstable = buildFakeSSTable(dir, 1, cols, false);

        SerializationHeader.Component header = readHeader(sstable);
        assertFrozenUdt(header, false);

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertFalse(headerFix.hasError());
        assertFalse(headerFix.hasChanges());
        assertTrue(updatedColumns.isEmpty());

        // must not have re-written the stats-component
        header = readHeader(sstable);
        assertFrozenUdt(header, false);
    }

    @Test
    public void compositePartitionKey() throws Exception
    {
        TableMetadata.Builder cols = TableMetadata.builder("ks", "cf")
                                                  .addPartitionKeyColumn("pk1", UTF8Type.instance)
                                                  .addPartitionKeyColumn("pk2", udtPK)
                                                  .addClusteringColumn("ck", udtCK);
        commonColumns(cols);

        File dir = temporaryFolder;
        File sstable = buildFakeSSTable(dir, 1, cols, true);

        SerializationHeader.Component header = readHeader(sstable);
        assertTrue(header.getKeyType() instanceof CompositeType);
        CompositeType keyType = (CompositeType) header.getKeyType();
        assertEquals(Arrays.asList(UTF8Type.instance, udtPK), keyType.getComponents());

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertFalse(headerFix.hasError());
        assertTrue(headerFix.hasChanges());
        assertEquals(Sets.newHashSet("pk2", "ck", "regular_b", "static_b"), updatedColumns);

        header = readHeader(sstable);
        assertTrue(header.getKeyType() instanceof CompositeType);
        keyType = (CompositeType) header.getKeyType();
        assertEquals(Arrays.asList(UTF8Type.instance, udtPK.freeze()), keyType.getComponents());
    }

    @Test
    public void compositeClusteringKey() throws Exception
    {
        TableMetadata.Builder cols = TableMetadata.builder("ks", "cf")
                                                  .addPartitionKeyColumn("pk", udtPK)
                                                  .addClusteringColumn("ck1", Int32Type.instance)
                                                  .addClusteringColumn("ck2", udtCK);
        commonColumns(cols);

        File dir = temporaryFolder;
        File sstable = buildFakeSSTable(dir, 1, cols, true);

        SerializationHeader.Component header = readHeader(sstable);
        assertEquals(Arrays.asList(Int32Type.instance, udtCK), header.getClusteringTypes());

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertFalse(headerFix.hasError());
        assertTrue(headerFix.hasChanges());
        assertEquals(Sets.newHashSet("pk", "ck2", "regular_b", "static_b"), updatedColumns);

        header = readHeader(sstable);
        assertEquals(Arrays.asList(Int32Type.instance, udtCK.freeze()), header.getClusteringTypes());
    }

    /**
     * Check whether {@link SSTableHeaderFix} can operate on a single file.
     */
    @Test
    public void singleFileUDTFixTest() throws Exception
    {
        File dir = temporaryFolder;
        File sstable = generateFakeSSTable(dir, 1);

        SerializationHeader.Component header = readHeader(sstable);
        assertFrozenUdt(header, false);

        SSTableHeaderFix headerFix = builder().withPath(sstable.toPath())
                                              .build();
        headerFix.execute();

        assertTrue(headerFix.hasChanges());
        assertFalse(headerFix.hasError());

        header = readHeader(sstable);
        assertFrozenUdt(header, true);
    }

    /**
     * Check whether {@link SSTableHeaderFix} can operate on a file in a directory.
     */
    @Test
    public void singleDirectoryUDTFixTest() throws Exception
    {
        File dir = temporaryFolder;
        List<File> sstables = IntStream.range(1, 11)
                                       .mapToObj(g -> generateFakeSSTable(dir, g))
                                       .collect(Collectors.toList());

        for (File sstable : sstables)
        {
            SerializationHeader.Component header = readHeader(sstable);
            assertFrozenUdt(header, false);
        }

        SSTableHeaderFix headerFix = builder().withPath(dir.toPath())
                                              .build();
        headerFix.execute();

        assertTrue(headerFix.hasChanges());
        assertFalse(headerFix.hasError());

        for (File sstable : sstables)
        {
            SerializationHeader.Component header = readHeader(sstable);
            assertFrozenUdt(header, true);
        }
    }

    /**
     * Check whether {@link SSTableHeaderFix} can operate multiple, single files.
     */
    @Test
    public void multipleFilesUDTFixTest() throws Exception
    {
        File dir = temporaryFolder;
        List<File> sstables = IntStream.range(1, 11)
                                       .mapToObj(g -> generateFakeSSTable(dir, g))
                                       .collect(Collectors.toList());

        for (File sstable : sstables)
        {
            SerializationHeader.Component header = readHeader(sstable);
            assertFrozenUdt(header, false);
        }

        SSTableHeaderFix.Builder builder = builder();
        sstables.stream().map(File::toPath).forEach(builder::withPath);
        SSTableHeaderFix headerFix = builder.build();
        headerFix.execute();

        assertTrue(headerFix.hasChanges());
        assertFalse(headerFix.hasError());

        for (File sstable : sstables)
        {
            SerializationHeader.Component header = readHeader(sstable);
            assertFrozenUdt(header, true);
        }
    }

    /**
     * Check whether {@link SSTableHeaderFix} can operate multiple files in a directory.
     */
    @Test
    public void multipleFilesInDirectoryUDTFixTest() throws Exception
    {
        File dir = temporaryFolder;
        List<File> sstables = IntStream.range(1, 11)
                                       .mapToObj(g -> generateFakeSSTable(dir, g))
                                       .collect(Collectors.toList());

        for (File sstable : sstables)
        {
            SerializationHeader.Component header = readHeader(sstable);
            assertFrozenUdt(header, false);
        }

        SSTableHeaderFix headerFix = builder().withPath(dir.toPath())
                                              .build();
        headerFix.execute();

        assertTrue(headerFix.hasChanges());
        assertFalse(headerFix.hasError());

        for (File sstable : sstables)
        {
            SerializationHeader.Component header = readHeader(sstable);
            assertFrozenUdt(header, true);
        }
    }

    private static final Pattern p = Pattern.compile(".* Column '([^']+)' needs to be updated from type .*");

    private SSTableHeaderFix.Builder builder()
    {
        updatedColumns.clear();
        return SSTableHeaderFix.builder()
                               .schemaCallback(() -> (desc) -> tableMetadata)
                               .info(ln -> {
                                   System.out.println("INFO: " + ln);
                                   Matcher m = p.matcher(ln);
                                   if (m.matches())
                                       updatedColumns.add(m.group(1));
                               })
                               .warn(ln -> System.out.println("WARN: " + ln))
                               .error(ln -> System.out.println("ERROR: " + ln));
    }

    private File generateFakeSSTable(File dir, int generation)
    {
        TableMetadata.Builder cols = TableMetadata.builder("ks", "cf")
                                                  .addPartitionKeyColumn("pk", udtPK)
                                                  .addClusteringColumn("ck", udtCK);
        commonColumns(cols);
        return buildFakeSSTable(dir, generation, cols, true);
    }

    private void commonColumns(TableMetadata.Builder cols)
    {
        cols.addRegularColumn("regular_a", UTF8Type.instance)
            .addRegularColumn("regular_b", udtRegular)
            .addRegularColumn("regular_c", Int32Type.instance)
            .addStaticColumn("static_a", UTF8Type.instance)
            .addStaticColumn("static_b", udtStatic)
            .addStaticColumn("static_c", Int32Type.instance);
    }

    private File buildFakeSSTable(File dir, int generation, TableMetadata.Builder cols, boolean freezeInSchema)
    {
        TableMetadata headerMetadata = cols.build();

        if (freezeInSchema)
        {
            TableMetadata.Builder schemaCols = TableMetadata.builder("ks", "cf");
            for (ColumnMetadata cm : cols.columns())
                schemaCols.addColumn(cm.withNewType(freezeUdt(cm.type)));
            tableMetadata = schemaCols.build();
        }
        else
        {
            tableMetadata = headerMetadata;
        }

        try
        {

            Descriptor desc = new Descriptor(version, dir, "ks", "cf", generation, SSTableFormat.Type.BIG);

            // Just create the component files - we don't really need those.
            for (Component component : requiredComponents)
                assertTrue(new File(desc.filenameFor(component)).createNewFile());

            AbstractType<?> partitionKey = headerMetadata.partitionKeyType;
            List<AbstractType<?>> clusteringKey = headerMetadata.clusteringColumns()
                                                                .stream()
                                                                .map(cd -> cd.type)
                                                                .collect(Collectors.toList());
            Map<ByteBuffer, AbstractType<?>> staticColumns = headerMetadata.columns()
                                                                           .stream()
                                                                           .filter(cd -> cd.kind == ColumnMetadata.Kind.STATIC)
                                                                           .collect(Collectors.toMap(cd -> cd.name.bytes, cd -> cd.type, (a, b) -> a));
            Map<ByteBuffer, AbstractType<?>> regularColumns = headerMetadata.columns()
                                                                            .stream()
                                                                            .filter(cd -> cd.kind == ColumnMetadata.Kind.REGULAR)
                                                                            .collect(Collectors.toMap(cd -> cd.name.bytes, cd -> cd.type, (a, b) -> a));

            File statsFile = new File(desc.filenameFor(Component.STATS));
            SerializationHeader.Component header = SerializationHeader.Component.buildComponentForTools(partitionKey,
                                                                                                        clusteringKey,
                                                                                                        staticColumns,
                                                                                                        regularColumns,
                                                                                                        EncodingStats.NO_STATS);

            try (SequentialWriter out = new SequentialWriter(statsFile))
            {
                desc.getMetadataSerializer().serialize(Collections.singletonMap(MetadataType.HEADER, header), out, version);
                out.finish();
            }

            return new File(desc.filenameFor(Component.DATA));
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    private AbstractType<?> freezeUdt(AbstractType<?> type)
    {
        if (type instanceof CollectionType)
        {
            if (type.getClass() == ListType.class)
            {
                ListType<?> cHeader = (ListType<?>) type;
                return ListType.getInstance(freezeUdt(cHeader.getElementsType()), cHeader.isMultiCell());
            }
            else if (type.getClass() == SetType.class)
            {
                SetType<?> cHeader = (SetType<?>) type;
                return SetType.getInstance(freezeUdt(cHeader.getElementsType()), cHeader.isMultiCell());
            }
            else if (type.getClass() == MapType.class)
            {
                MapType<?, ?> cHeader = (MapType<?, ?>) type;
                return MapType.getInstance(freezeUdt(cHeader.getKeysType()), freezeUdt(cHeader.getValuesType()), cHeader.isMultiCell());
            }
        }
        else if (type instanceof AbstractCompositeType)
        {
            if (type.getClass() == CompositeType.class)
            {
                CompositeType cHeader = (CompositeType) type;
                return CompositeType.getInstance(cHeader.types.stream().map(this::freezeUdt).collect(Collectors.toList()));
            }
        }
        else if (type instanceof TupleType)
        {
            if (type.getClass() == UserType.class)
            {
                UserType cHeader = (UserType) type;
                cHeader = cHeader.freeze();
                return new UserType(cHeader.keyspace, cHeader.name, cHeader.fieldNames(),
                                    cHeader.allTypes().stream().map(this::freezeUdt).collect(Collectors.toList()),
                                    cHeader.isMultiCell());
            }
        }
        return type;
    }

    private void assertFrozenUdt(SerializationHeader.Component header, boolean frozen)
    {
        AbstractType<?> keyType = header.getKeyType();
        if (keyType instanceof CompositeType)
        {
            for (AbstractType<?> component : ((CompositeType) keyType).types)
                assertFrozenUdt(component, frozen);
        }
        assertFrozenUdt(keyType, frozen);

        for (AbstractType<?> type : header.getClusteringTypes())
            assertFrozenUdt(type, frozen);
        for (AbstractType<?> type : header.getStaticColumns().values())
            assertFrozenUdt(type, frozen);
        for (AbstractType<?> type : header.getRegularColumns().values())
            assertFrozenUdt(type, frozen);
    }

    private void assertFrozenUdt(AbstractType<?> type, boolean frozen)
    {
        if (type instanceof CompositeType)
        {
            for (AbstractType<?> component : ((CompositeType) type).types)
                assertFrozenUdt(component, frozen);
        }
        else if (type instanceof CollectionType)
        {
            if (type instanceof MapType)
            {
                MapType map = (MapType) type;
                // only descend for non-frozen types (checking frozen in frozen is just stupid)
                if (map.isMultiCell())
                {
                    assertFrozenUdt(map.getKeysType(), frozen);
                    assertFrozenUdt(map.getValuesType(), frozen);
                }
            }
            else if (type instanceof SetType)
            {
                SetType set = (SetType) type;
                // only descend for non-frozen types (checking frozen in frozen is just stupid)
                if (set.isMultiCell())
                    assertFrozenUdt(set.getElementsType(), frozen);
            }
            else if (type instanceof ListType)
            {
                ListType list = (ListType) type;
                // only descend for non-frozen types (checking frozen in frozen is just stupid)
                if (list.isMultiCell())
                    assertFrozenUdt(list.getElementsType(), frozen);
            }
        }
        else if (type instanceof TupleType)
        {
            TupleType tuple = (TupleType) type;
            // only descend for non-frozen types (checking frozen in frozen is just stupid)
            if (tuple.isMultiCell())
                for (AbstractType<?> component : tuple.allTypes())
                    assertFrozenUdt(component, frozen);
        }

        if (type instanceof UserType)
        {
            String typeString = type.toString();
            assertEquals(typeString, frozen, !type.isMultiCell());
            if (typeString.startsWith(UserType.class.getName() + '('))
                if (frozen)
                    fail(typeString);
            if (typeString.startsWith(FrozenType.class.getName() + '(' + UserType.class.getName() + '('))
                if (!frozen)
                    fail(typeString);
        }
    }

    private SerializationHeader.Component readHeader(File sstable) throws Exception
    {
        Descriptor desc = Descriptor.fromFilename(sstable);
        return (SerializationHeader.Component) desc.getMetadataSerializer().deserialize(desc, MetadataType.HEADER);
    }

    private static final Component[] requiredComponents = new Component[]{ Component.DATA, Component.FILTER, Component.PRIMARY_INDEX, Component.TOC };
}
