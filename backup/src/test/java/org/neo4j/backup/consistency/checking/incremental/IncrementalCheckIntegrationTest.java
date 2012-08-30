package org.neo4j.backup.consistency.checking.incremental;

import java.io.IOException;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.backup.consistency.ConsistencyCheckingError;
import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.log.VerifyingTransactionInterceptorProvider;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.UTF8;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.LongerShortString;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.test.GraphStoreFixture;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

public class IncrementalCheckIntegrationTest
{
    @Test
    public void shouldReportNodeInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.NODE, new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                tx.create( new NodeRecord( next.node(), next.relationship(), -1 ) );
            }
        } );
    }

    @Test
    public void shouldReportRelationshipInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.RELATIONSHIP, new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                long node = next.node();
                tx.create( new RelationshipRecord( next.relationship(), node, node, 0 ) );
            }
        } );
    }

    @Test
    public void shouldReportPropertyInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.PROPERTY, new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                PropertyRecord property = new PropertyRecord( next.property() );
                property.setPrevProp( next.property() );

                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( (((long) PropertyType.INT.intValue()) << 24) | (666 << 28) );
                property.addPropertyBlock( block );

                tx.create( property );
            }
        } );
    }

    @Test
    public void shouldReportStringPropertyInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.STRING_PROPERTY, new GraphStoreFixture.Transaction()
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
    }

    @Test
    public void shouldReportArrayPropertyInconsistency() throws Exception
    {
        verifyInconsistencyReported( RecordType.ARRAY_PROPERTY, new GraphStoreFixture.Transaction()
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
    }

    private static String LONG_STRING, LONG_SHORT_STRING;

    static
    {
        StringBuilder longString = new StringBuilder();
        String longShortString = "";
        for ( int i = 0; LongerShortString.encode( 0, longString.toString(), new PropertyBlock(),
                                                   PropertyType.getPayloadSize() ); i++ )
        {
            longShortString = longString.toString();
            longString.append( 'a' + (i % ('z' - 'a')) );
        }
        LONG_SHORT_STRING = longShortString;
        LONG_STRING = longString.toString();
    }

    @Rule
    public final GraphStoreFixture fixture = new GraphStoreFixture()
    {
        @Override
        protected void generateInitialData( GraphDatabaseService graphDb )
        {
            org.neo4j.graphdb.Transaction tx = graphDb.beginTx();
            try
            {
                Node node1 = set( graphDb.createNode(),
                                  property( "long short short", LONG_SHORT_STRING ) );
                Node node2 = set( graphDb.createNode(),
                                  property( "long string", LONG_STRING ) );
                Node node3 = set( graphDb.createNode(),
                                  property( "one", "1" ),
                                  property( "two", "2" ),
                                  property( "three", "3" ),
                                  property( "four", "4" ),
                                  property( "five", "5" ) );
                Node node4 = set( graphDb.createNode(),
                                  property( "name", "Leeloo Dallas" ) );
                Node node5 = set( graphDb.createNode(),
                                  property( "payload", LONG_SHORT_STRING ),
                                  property( "more", LONG_STRING ) );
                Node node6 = set( graphDb.createNode() );

                set( node1.createRelationshipTo( node2, withName( "WHEEL" ) ) );
                set( node2.createRelationshipTo( node3, withName( "WHEEL" ) ) );
                set( node3.createRelationshipTo( node4, withName( "WHEEL" ) ) );
                set( node4.createRelationshipTo( node5, withName( "WHEEL" ) ) );
                set( node5.createRelationshipTo( node1, withName( "WHEEL" ) ) );

                set( node6.createRelationshipTo( node1, withName( "STAR" ) ) );
                set( node6.createRelationshipTo( node2, withName( "STAR" ) ) );
                set( node6.createRelationshipTo( node3, withName( "STAR" ) ) );
                set( node6.createRelationshipTo( node4, withName( "STAR" ) ) );
                set( node6.createRelationshipTo( node5, withName( "STAR" ) ) );

                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }

        @Override
        protected Map<String, String> configuration( boolean initialData )
        {
            Map<String, String> config = super.configuration( initialData );
            if ( !initialData )
            {
                config.put( GraphDatabaseSettings.intercept_deserialized_transactions.name(), "true" );
                config.put( GraphDatabaseSettings.intercept_committing_transactions.name(), "true" );
                config.put( TransactionInterceptorProvider.class.getSimpleName() + "." +
                                    VerifyingTransactionInterceptorProvider.NAME, "true" );
            }
            return config;
        }
    };

    private void verifyInconsistencyReported( RecordType recordType,
                                              GraphStoreFixture.Transaction inconsistentTransaction )
            throws IOException
    {
        // when
        try
        {
            fixture.apply( inconsistentTransaction );
            fail( "should have thrown error" );
        }
        // then
        catch ( ConsistencyCheckingError expected )
        {
            int count = expected.getInconsistencyCountForRecordType( recordType );
            int total = expected.getTotalInconsistencyCount();
            assertTrue( "Expected failures for " + recordType, count > 0 );
            assertEquals( "Didn't expect failures for any other type than " + recordType, total, count );
        }
    }
}
