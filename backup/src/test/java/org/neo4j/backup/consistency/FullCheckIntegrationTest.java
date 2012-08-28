package org.neo4j.backup.consistency;

import java.io.StringWriter;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;
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
import static org.junit.Assert.assertThat;
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

    @Test
    public void shouldCheckConsistencyOfAConsistentStore() throws Exception
    {
        // given
        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( fixture.storeAccess() );

        // then
        assertEquals( "", writer.toString() );
    }

    @Test
    public void shouldReportNodeInconsistencies() throws Exception
    {
        // given
        final Reference<Long> inconsistentNode = new Reference<Long>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentNode.set( next.node() );
                tx.create( new NodeRecord( inconsistentNode.get(), next.relationship(), -1 ) );
            }
        } );
        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( fixture.storeAccess() );

        // then
        String logged = writer.toString();
        assertTrue( "Expected inconsistencies", logged.trim().length() > 0 );
        assertThat( logged, containsOnEachLine( "Node[" + inconsistentNode ) );
    }

    @Test
    public void shouldReportRelationshipInconsistencies() throws Exception
    {
        // given
        final Reference<Long> inconsistentRelationship = new Reference<Long>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentRelationship.set( next.relationship() );
                tx.create( new RelationshipRecord( inconsistentRelationship.get(), 1, 2, 0 ) );
            }
        } );
        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( fixture.storeAccess() );

        // then
        String logged = writer.toString();
        assertTrue( "Expected inconsistencies", logged.trim().length() > 0 );
        assertThat( logged, containsOnEachLine( "Relationship[" + inconsistentRelationship ) );
    }

    @Test
    public void shouldReportPropertyInconsistencies() throws Exception
    {
        // given
        final Reference<Long> inconsistentProperty = new Reference<Long>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentProperty.set( next.property() );
                PropertyRecord property = new PropertyRecord( inconsistentProperty.get() );
                property.setPrevProp( next.property() );
                property.setNextProp( next.property() );
                PropertyBlock block = new PropertyBlock();
                block.setSingleBlock( 1 | (((long) PropertyType.INT.intValue()) << 24) | (666 << 28) );
                property.addPropertyBlock( block );
                tx.create( property );
            }
        } );
        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( fixture.storeAccess() );

        // then
        String logged = writer.toString();
        assertTrue( "Expected inconsistencies", logged.trim().length() > 0 );
        assertThat( logged, containsOnEachLine( "Property[" + inconsistentProperty ) );
    }

    @Test
    public void shouldReportStringPropertyInconsistencies() throws Exception
    {
        // given
        final Reference<Long> inconsistentString = new Reference<Long>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentString.set( next.stringProperty() );
                DynamicRecord string = new DynamicRecord( inconsistentString.get() );
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
        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( fixture.storeAccess() );

        // then
        String logged = writer.toString();
        assertTrue( "Expected inconsistencies", logged.trim().length() > 0 );
        assertThat( logged, containsOnEachLine( "DynamicRecord[" + inconsistentString ) );
    }

    @Test
    public void shouldReportArrayPropertyInconsistencies() throws Exception
    {
        // given
        final Reference<Long> inconsistentArray = new Reference<Long>();
        fixture.apply( new GraphStoreFixture.Transaction()
        {
            @Override
            protected void transactionData( GraphStoreFixture.TransactionDataBuilder tx,
                                            GraphStoreFixture.IdGenerator next )
            {
                inconsistentArray.set( next.arrayProperty() );
                DynamicRecord array = new DynamicRecord( inconsistentArray.get() );
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
        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( fixture.storeAccess() );

        // then
        String logged = writer.toString();
        assertTrue( "Expected inconsistencies", logged.trim().length() > 0 );
        assertThat( logged, containsOnEachLine( "DynamicRecord[" + inconsistentArray ) );
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

        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( access );

        // then
        String logged = writer.toString();
        assertTrue( "Expected inconsistencies", logged.trim().length() > 0 );
        assertThat( logged, containsOnEachLine( "DynamicRecord[" + inconsistentName ) );
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

        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( access );

        // then
        String logged = writer.toString();
        assertTrue( "Expected inconsistencies", logged.trim().length() > 0 );
        assertThat( logged, containsOnEachLine( "DynamicRecord[" + inconsistentName ) );
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

        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( access );

        // then
        String logged = writer.toString();
        assertTrue( "Expected inconsistencies", logged.trim().length() > 0 );
        assertThat( logged, containsOnEachLine( "RelationshipTypeRecord[" + 1 ) );
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

        StringWriter writer = new StringWriter();
        FullCheck check = new FullCheck( true, Progress.Factory.NONE, StringLogger.wrap( writer ) );

        // when
        check.execute( access );

        // then
        String logged = writer.toString();
        assertTrue( "Expected inconsistencies", logged.trim().length() > 0 );
        assertThat( logged, containsOnEachLine( "PropertyIndexRecord[" + inconsistentKey ) );
    }

    private Matcher<String> containsOnEachLine( final String fragment )
    {
        return new TypeSafeMatcher<String>()
        {
            @Override
            public boolean matchesSafely( String item )
            {
                for ( String line : item.split( "\n" ) )
                {
                    line = line.trim();
                    if ( !"".equals( line ) )
                    {
                        if ( !(line.contains( "ERROR: " + fragment ) || line.contains( "WARNING: " + fragment )) )
                        {
                            return false;
                        }
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "String where each line contains " ).appendValue( fragment );
            }
        };
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
