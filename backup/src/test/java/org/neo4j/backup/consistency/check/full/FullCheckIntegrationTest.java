package org.neo4j.backup.consistency.check.full;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.report.ConsistencyReporter;
import org.neo4j.backup.consistency.report.ConsistencySummaryStats;
import org.neo4j.backup.consistency.store.DirectReferenceDispatcher;
import org.neo4j.backup.consistency.store.SimpleRecordAccess;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.Progress;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.GraphStoreFixture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

public class FullCheckIntegrationTest
{
    @Rule
    public final GraphStoreFixture fixture = new GraphStoreFixture()
    {
        @Override
        protected void generateInitialData( GraphDatabaseService graphDb )
        {
            org.neo4j.graphdb.Transaction tx = graphDb.beginTx();
            try
            {
                Node node1 = set( graphDb.createNode() );
                Node node2 = set( graphDb.createNode(), property( "key", "value" ) );
                node1.createRelationshipTo( node2, DynamicRelationshipType.withName( "C" ) );
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }
    };

    private ConsistencySummaryStats check()
    {
        return check( fixture.storeAccess() );
    }

    private ConsistencySummaryStats check( StoreAccess access )
    {
        ConsistencyReporter.SummarisingReporter reporter = ConsistencyReporter
                .create( new SimpleRecordAccess( access ),
                         new DirectReferenceDispatcher(),
                         StringLogger.DEV_NULL );
        FullCheck checker = new FullCheck( true, Progress.Factory.NONE );
        checker.execute( access, reporter );
        return reporter.getSummary();
    }

    private void verifyInconsistency( RecordType recordType, ConsistencySummaryStats stats )
    {
        int count = stats.getInconsistencyCountForRecordType( recordType );
        assertTrue( "Expected inconsistencies for records of type: " + recordType, count > 0 );
        assertEquals( "Expected only inconsistencies of type: " + recordType,
                      count, stats.getTotalInconsistencyCount() );
    }

    @Test
    public void shouldCheckConsistencyOfAConsistentStore() throws Exception
    {
        // when
        ConsistencySummaryStats result = check();

        // then
        assertEquals( 0, result.getTotalInconsistencyCount() );
    }

    @Test
    public void shouldReportNodeInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), next.relationship(), -1 ) );
            }
        } );

        // when
        ConsistencySummaryStats stats = check();

        // then
        verifyInconsistency( RecordType.NODE, stats );
    }

    @Test
    public void shouldReportRelationshipInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.create( new RelationshipRecord( next.relationship(), 1, 2, 0 ) );
            }
        } );

        // when
        ConsistencySummaryStats stats = check();

        // then
        verifyInconsistency( RecordType.RELATIONSHIP, stats );
    }

    @Test
    public void shouldReportPropertyInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                PropertyRecord property = new PropertyRecord( next.property() );
                property.setPrevProp( next.property() );
                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( 1 | (((long) PropertyType.INT.intValue()) << 24) | (666 << 28) );
                property.addPropertyBlock( block );
                tx.create( property );
            }
        } );

        // when
        ConsistencySummaryStats stats = check();

        // then
        verifyInconsistency( RecordType.PROPERTY, stats );
    }

    @Test
    public void shouldReportStringPropertyInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord string = new DynamicRecord( next.stringProperty() );
                string.setInUse( true );
                string.setCreated();
                string.setType( PropertyType.STRING.intValue() );
                string.setNextBlock( next.stringProperty() );
                string.setData( UTF8.encode( "hello world" ) );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( (((long) PropertyType.STRING.intValue()) << 24) | (string.getId() << 28) );
                block.addValueRecord( string );

                PropertyRecord property = new PropertyRecord( next.property() );
                property.addPropertyBlock( block );

                tx.create( property );
            }
        } );

        // when
        ConsistencySummaryStats stats = check();

        // then
        verifyInconsistency( RecordType.STRING_PROPERTY, stats );
    }

    @Test
    public void shouldReportArrayPropertyInconsistencies() throws Exception
    {
        // given
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                DynamicRecord array = new DynamicRecord( next.arrayProperty() );
                array.setInUse( true );
                array.setCreated();
                array.setType( PropertyType.ARRAY.intValue() );
                array.setNextBlock( next.arrayProperty() );
                array.setData( UTF8.encode( "hello world" ) );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( (((long) PropertyType.ARRAY.intValue()) << 24) | (array.getId() << 28) );
                block.addValueRecord( array );

                PropertyRecord property = new PropertyRecord( next.property() );
                property.addPropertyBlock( block );

                tx.create( property );
            }
        } );

        // when
        ConsistencySummaryStats stats = check();

        // then
        verifyInconsistency( RecordType.ARRAY_PROPERTY, stats );
    }

    @Test
    public void shouldReportRelationshipLabelNameInconsistencies() throws Exception
    {
        // given
        final Reference<Integer> inconsistentName = new Reference<Integer>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentName.set( next.relationshipType() );
                tx.relationshipType( inconsistentName.get(), "FOO" );
            }
        } );
        StoreAccess access = fixture.storeAccess();
        DynamicRecord record = access.getTypeNameStore().forceGetRecord( inconsistentName.get() );
        record.setNextBlock( record.getId() );
        access.getTypeNameStore().updateRecord( record );

        // when
        ConsistencySummaryStats stats = check( access );

        // then
        verifyInconsistency( RecordType.RELATIONSHIP_LABEL_NAME, stats );
    }

    @Test
    public void shouldReportPropertyKeyNameInconsistencies() throws Exception
    {
        // given
        final Reference<Integer> inconsistentName = new Reference<Integer>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentName.set( next.propertyKey() );
                tx.propertyKey( inconsistentName.get(), "FOO" );
            }
        } );
        StoreAccess access = fixture.storeAccess();
        DynamicRecord record = access.getPropertyKeyStore().forceGetRecord( inconsistentName.get() );
        record.setNextBlock( record.getId() );
        access.getPropertyKeyStore().updateRecord( record );

        // when
        ConsistencySummaryStats stats = check( access );

        // then
        verifyInconsistency( RecordType.PROPERTY_KEY_NAME, stats );
    }

    @Test
    public void shouldReportRelationshipLabelInconsistencies() throws Exception
    {
        // given
        StoreAccess access = fixture.storeAccess();
        RelationshipTypeRecord record = access.getRelationshipTypeStore().forceGetRecord( 1 );
        record.setNameId( 20 );
        record.setInUse( true );
        access.getRelationshipTypeStore().updateRecord( record );

        // when
        ConsistencySummaryStats stats = check( access );

        // then
        verifyInconsistency( RecordType.RELATIONSHIP_LABEL, stats );
    }

    @Test
    public void shouldReportPropertyKeyInconsistencies() throws Exception
    {
        // given
        final Reference<Integer> inconsistentKey = new Reference<Integer>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentKey.set( next.propertyKey() );
                tx.propertyKey( inconsistentKey.get(), "FOO" );
            }
        } );
        StoreAccess access = fixture.storeAccess();
        DynamicRecord record = access.getPropertyKeyStore().forceGetRecord( inconsistentKey.get() );
        record.setInUse( false );
        access.getPropertyKeyStore().updateRecord( record );

        // when
        ConsistencySummaryStats stats = check( access );

        // then
        verifyInconsistency( RecordType.PROPERTY_KEY, stats );
    }

    private static class Reference<T>
    {
        private T value;

        void set(T value)
        {
            this.value = value;
        }

        T get()
        {
            return value;
        }

        @Override
        public String toString()
        {
            return String.valueOf( value );
        }
    }
}
