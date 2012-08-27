package org.neo4j.backup.consistency.check;

import java.util.ArrayList;
import java.util.Collection;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

abstract class RecordCheckTestBase<RECORD extends AbstractBaseRecord,
        REPORT extends ConsistencyReport<RECORD, REPORT>,
        CHECKER extends RecordCheck<RECORD, REPORT>>
{
    static final int NONE = -1;
    private final CHECKER checker;
    private final Class<REPORT> reportClass;

    RecordCheckTestBase( CHECKER checker, Class<REPORT> reportClass )
    {
        this.checker = checker;
        this.reportClass = reportClass;
    }

    final REPORT check( RECORD record, RecordAccess records )
    {
        return check( reportClass, checker, record, records );
    }

    static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    REPORT check( Class<REPORT> reportClass, RecordCheck<RECORD, REPORT> checker, RECORD record, RecordAccess records )
    {
        REPORT report = mock( reportClass );
        Collection<Runnable> deferred = DeferredReferenceCheck.stub( record, report );
        checker.check( record, report, new RecordReferencer( records ) );
        for ( Runnable check : deferred )
        {
            check.run();
        }
        return report;
    }

    static <R extends AbstractBaseRecord> R add( RecordAccess mock, R record )
    {
        if ( record instanceof NodeRecord )
        {
            when( mock.getNode( record.getLongId() ) ).thenReturn( (NodeRecord) record );
        }
        else if ( record instanceof RelationshipRecord )
        {
            when( mock.getRelationship( record.getLongId() ) ).thenReturn( (RelationshipRecord) record );
        }
        else if ( record instanceof PropertyRecord )
        {
            when( mock.getProperty( record.getLongId() ) ).thenReturn( (PropertyRecord) record );
        }
        else if ( record instanceof RelationshipTypeRecord )
        {
            when( mock.getType( (int) record.getLongId() ) ).thenReturn( (RelationshipTypeRecord) record );
        }
        else if ( record instanceof PropertyIndexRecord )
        {
            when( mock.getKey( (int) record.getLongId() ) ).thenReturn( (PropertyIndexRecord) record );
        }
        else if ( record instanceof DynamicRecord )
        {
            DynamicRecord dyn = (DynamicRecord) record;
            if ( dyn.getType() == PropertyType.STRING.intValue() )
            {
                when( mock.getString( record.getLongId() ) ).thenReturn( dyn );
            }
            else if ( dyn.getType() == PropertyType.ARRAY.intValue() )
            {
                when( mock.getArray( record.getLongId() ) ).thenReturn( dyn );
            }
            else
            {
                fail( "Dynamic record must be either STRING or ARRAY" );
            }
        }
        else
        {
            fail( "Unknown record type: " + record );
        }
        return record;
    }

    static DynamicRecord string( DynamicRecord record )
    {
        record.setType( PropertyType.STRING.intValue() );
        return record;
    }

    static DynamicRecord array( DynamicRecord record )
    {
        record.setType( PropertyType.ARRAY.intValue() );
        return record;
    }

    static PropertyBlock propertyBlock( PropertyIndexRecord key, DynamicRecord value )
    {
        PropertyType type;
        if ( value.getType() == PropertyType.STRING.intValue() )
        {
            type = PropertyType.STRING;
        }
        else if ( value.getType() == PropertyType.ARRAY.intValue() )
        {
            type = PropertyType.ARRAY;
        }
        else
        {
            fail( "Dynamic record must be either STRING or ARRAY" );
            return null;
        }
        return propertyBlock( key, type, value.getId() );
    }

    static PropertyBlock propertyBlock( PropertyIndexRecord key, PropertyType type, long value )
    {
        PropertyBlock block = new PropertyBlock();
        block.setSingleBlock( key.getId() | (((long) type.intValue()) << 24) | (value << 28) );
        return block;
    }

    static <R extends AbstractBaseRecord> R inUse( R record )
    {
        record.setInUse( true );
        return record;
    }

    static <R extends AbstractBaseRecord> R notInUse( R record )
    {
        record.setInUse( false );
        return record;
    }

    @SuppressWarnings("unchecked")
    static void verifyOnlyReferenceDispatch( ConsistencyReport report )
    {
        verify( report, atLeast( 0 ) )
                .forReference( any( RecordReference.class ), any( ComparativeRecordChecker.class ) );
        verifyNoMoreInteractions( report );
    }

    @SuppressWarnings("unchecked")
    private static class DeferredReferenceCheck<RECORD extends AbstractBaseRecord,
            REPORT extends ConsistencyReport<RECORD, REPORT>>
            implements Runnable
    {
        static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
        Collection<Runnable> stub( final RECORD record, final REPORT mock )
        {
            final ArrayList<Runnable> deferred = new ArrayList<Runnable>();
            doAnswer( new Answer()
            {
                @Override
                public Object answer( InvocationOnMock invocation ) throws Throwable
                {
                    deferred.add( deferredCheck( record, mock, invocation.getArguments() ) );
                    return null;
                }
            } ).when( mock ).forReference( any( RecordReference.class ), any( ComparativeRecordChecker.class ) );
            return deferred;
        }

        static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
        DeferredReferenceCheck deferredCheck( RECORD record, REPORT report, Object[] arguments )
        {
            return new DeferredReferenceCheck( record, report, (RecordReference) arguments[0],
                                               (ComparativeRecordChecker) arguments[1] );
        }

        private final RECORD record;
        private final REPORT report;
        private final RecordReference reference;
        private final ComparativeRecordChecker checker;

        private DeferredReferenceCheck( RECORD record, REPORT report, RecordReference reference,
                                        ComparativeRecordChecker checker )
        {
            this.record = record;
            this.report = report;
            this.reference = reference;
            this.checker = checker;
        }

        @Override
        public void run()
        {
            reference.dispatch( checker, record, report );
        }
    }
}
