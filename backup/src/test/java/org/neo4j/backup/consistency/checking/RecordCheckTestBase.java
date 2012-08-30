package org.neo4j.backup.consistency.checking;

import java.util.ArrayList;
import java.util.Collection;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
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

public abstract class RecordCheckTestBase<RECORD extends AbstractBaseRecord,
        REPORT extends ConsistencyReport<RECORD, REPORT>,
        CHECKER extends RecordCheck<RECORD, REPORT>>
{
    public static final int NONE = -1;
    private final CHECKER checker;
    private final Class<REPORT> reportClass;
    private final RecordAccess recordAccessMock;

    RecordCheckTestBase( CHECKER checker, Class<REPORT> reportClass )
    {
        this.checker = checker;
        this.reportClass = reportClass;
        this.recordAccessMock = mock( RecordAccess.class );
    }

    final REPORT check( RECORD record )
    {
        return check( reportClass, checker, record, recordAccessMock );
    }

    final REPORT checkChange( RECORD oldRecord, RECORD newRecord )
    {
        return checkChange( reportClass, checker, oldRecord, newRecord, recordAccessMock );
    }

    public static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    REPORT check( Class<REPORT> reportClass, RecordCheck<RECORD, REPORT> checker, RECORD record,
                  final RecordAccess records )
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

    static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    REPORT checkChange( Class<REPORT> reportClass, RecordCheck<RECORD, REPORT> checker,
                        RECORD oldRecord, RECORD newRecord, final RecordAccess records )
    {
        REPORT report = mock( reportClass );
        Collection<Runnable> deferred = DeferredReferenceDiffCheck.stub( oldRecord, newRecord, report );
        checker.checkChange( oldRecord, newRecord, report, new DiffRecordReferencer( records ) );
        for ( Runnable check : deferred )
        {
            check.run();
        }
        return report;
    }

    <R extends AbstractBaseRecord> R addChange( R oldRecord, R newRecord )
    {
        addToMock( recordAccessMock, oldRecord, newRecord );
        return newRecord;
    }

    <R extends AbstractBaseRecord> R add( R record )
    {
        addToMock( recordAccessMock, null, record );
        return record;
    }

    public static <R extends AbstractBaseRecord> R add( RecordAccess mock, R record )
    {
        addToMock( mock, null, record );
        return record;
    }

    DynamicRecord addKeyName( DynamicRecord name )
    {
        when( recordAccessMock.getKeyName( (int) name.getId() ) ).thenReturn( name );
        return name;
    }

    DynamicRecord addLabelName( DynamicRecord name )
    {
        when( recordAccessMock.getLabelName( (int) name.getId() ) ).thenReturn( name );
        return name;
    }

    private static void addToMock( RecordAccess mock, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord )
    {
        if ( newRecord instanceof NodeRecord )
        {
            if ( oldRecord != null )
            {
                when( mock.changedNode( oldRecord.getLongId() ) ).thenReturn( (NodeRecord) newRecord );
            }
            when( mock.getNode( newRecord.getLongId() ) ).thenReturn( (NodeRecord) newRecord );
        }
        else if ( newRecord instanceof RelationshipRecord )
        {
            if ( oldRecord != null )
            {
                when( mock.changedRelationship( oldRecord.getLongId() ) ).thenReturn( (RelationshipRecord) newRecord );
            }
            when( mock.getRelationship( newRecord.getLongId() ) ).thenReturn( (RelationshipRecord) newRecord );
        }
        else if ( newRecord instanceof PropertyRecord )
        {
            if ( oldRecord != null )
            {
                when( mock.changedProperty( oldRecord.getLongId() ) ).thenReturn( (PropertyRecord) newRecord );
            }
            when( mock.getProperty( newRecord.getLongId() ) ).thenReturn( (PropertyRecord) newRecord );
        }
        else if ( newRecord instanceof RelationshipTypeRecord )
        {
            when( mock.getType( (int) newRecord.getLongId() ) ).thenReturn( (RelationshipTypeRecord) newRecord );
        }
        else if ( newRecord instanceof PropertyIndexRecord )
        {
            when( mock.getKey( (int) newRecord.getLongId() ) ).thenReturn( (PropertyIndexRecord) newRecord );
        }
        else if ( newRecord instanceof DynamicRecord )
        {
            DynamicRecord dyn = (DynamicRecord) newRecord;
            if ( dyn.getType() == PropertyType.STRING.intValue() )
            {
                if ( oldRecord != null )
                {
                    when( mock.changedString( oldRecord.getLongId() ) ).thenReturn( (DynamicRecord) newRecord );
                }
                when( mock.getString( newRecord.getLongId() ) ).thenReturn( dyn );
            }
            else if ( dyn.getType() == PropertyType.ARRAY.intValue() )
            {
                if ( oldRecord != null )
                {
                    when( mock.changedArray( oldRecord.getLongId() ) ).thenReturn( (DynamicRecord) newRecord );
                }
                when( mock.getArray( newRecord.getLongId() ) ).thenReturn( dyn );
            }
            else
            {
                fail( "Dynamic record must be either STRING or ARRAY" );
            }
        }
        else
        {
            fail( "Unknown record type: " + newRecord );
        }
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

    public static <R extends AbstractBaseRecord> R inUse( R record )
    {
        record.setInUse( true );
        return record;
    }

    public static <R extends AbstractBaseRecord> R notInUse( R record )
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

    @SuppressWarnings("unchecked")
    private static class DeferredReferenceDiffCheck<RECORD extends AbstractBaseRecord,
            REPORT extends ConsistencyReport<RECORD, REPORT>>
            implements Runnable
    {
        static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
        Collection<Runnable> stub( final RECORD oldRecord,  final RECORD newRecord, final REPORT mock )
        {
            final ArrayList<Runnable> deferred = new ArrayList<Runnable>();
            doAnswer( new Answer()
            {
                @Override
                public Object answer( InvocationOnMock invocation ) throws Throwable
                {
                    deferred.add( deferredCheck( oldRecord, newRecord, mock, invocation.getArguments() ) );
                    return null;
                }
            } ).when( mock ).forReference( any( RecordReference.class ), any( ComparativeRecordChecker.class ) );
            return deferred;
        }

        static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
        DeferredReferenceDiffCheck deferredCheck(  final RECORD oldRecord,  final RECORD newRecord, REPORT report,
                                                   Object[] arguments )
        {
            return new DeferredReferenceDiffCheck( oldRecord, newRecord, report, (RecordReference) arguments[0],
                                               (ComparativeRecordChecker) arguments[1] );
        }

        private final RECORD oldRecord, newRecord;
        private final REPORT report;
        private final RecordReference reference;
        private final ComparativeRecordChecker checker;

        private DeferredReferenceDiffCheck( RECORD oldRecord, RECORD newRecord, REPORT report, RecordReference reference,
                                            ComparativeRecordChecker checker )
        {
            this.oldRecord = oldRecord;
            this.newRecord = newRecord;
            this.report = report;
            this.reference = reference;
            this.checker = checker;
        }

        @Override
        public void run()
        {
            reference.dispatch( checker, newRecord, report );
        }
    }
}
