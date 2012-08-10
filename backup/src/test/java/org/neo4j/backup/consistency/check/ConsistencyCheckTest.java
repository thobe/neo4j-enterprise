package org.neo4j.backup.consistency.check;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.backup.consistency.ConsistencyReport;
import org.neo4j.backup.consistency.InconsistencyType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.ProgressIndicator;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.LongerShortString;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.test.GraphStoreFixture;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.test.Property.property;
import static org.neo4j.test.Property.set;

public class ConsistencyCheckTest
{
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
    };

    @Test
    public void shouldReportNothingOnConsistentDatabase() throws Exception
    {
        // given
        ConsistencyReport report = mock( ConsistencyReport.class );

        // when
        check( report );

        // then
        verifyZeroInteractions( report );
    }

    @Test
    public void shouldReportNodeRecordReferencingRelationshipRecordNotInUse() throws Exception
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

        ConsistencyReport report = mock( ConsistencyReport.class );

        // when
        check( report );

        // then
        verify( report ).nodeRelationshipNotInUse( any( NodeRecord.class ), any( RelationshipRecord.class ) );
    }

    private void check( final ConsistencyReport report )
    {
        StoreAccess store = fixture.storeAccess();
        try
        {
            new ConsistencyRecordProcessor( store, new InconsistencyReport()
            {
                @Override
                public <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> boolean inconsistent(
                        RecordStore<R1> recordStore, R1 record, RecordStore<? extends R2> referredStore, R2 referred,
                        InconsistencyType type )
                {

                    if ( type == InconsistencyType.ReferenceInconsistency.RELATIONSHIP_NOT_IN_USE )
                    {
                        report.nodeRelationshipNotInUse( (NodeRecord) record, (RelationshipRecord) referred );
                    }
                    else
                    {
                        fail( "Unexpected inconsistency: " + type );
                    }
                    return !type.isWarning();
                }

                @Override
                public <R extends AbstractBaseRecord> boolean inconsistent( RecordStore<R> store, R record, R referred,
                                                                            InconsistencyType type )
                {
                    fail( "Unexpected inconsistency: " + type );
                    return !type.isWarning();
                }

                @Override
                public <R extends AbstractBaseRecord> boolean inconsistent( RecordStore<R> store, R record,
                                                                            InconsistencyType type )
                {
                    fail( "Unexpected inconsistency: " + type );
                    return !type.isWarning();
                }
            }, ProgressIndicator.Factory.NONE ).run();
        }
        finally
        {
            store.close();
        }
    }
}
