package org.neo4j.backup.consistency.check;

import java.util.Arrays;

import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

abstract class PrimitiveRecordCheck
        <RECORD extends PrimitiveRecord, REPORT extends ConsistencyReport.PrimitiveConsistencyReport<RECORD, REPORT>>
        implements RecordCheck<RECORD, REPORT>
{
    private final RecordField<RECORD, REPORT>[] fields;
    final ComparativeRecordChecker<RECORD, PrimitiveRecord, REPORT> ownerCheck =
            new ComparativeRecordChecker<RECORD, PrimitiveRecord, REPORT>()
            {
                @Override
                public void checkReference( RECORD record, PrimitiveRecord other, REPORT report )
                {
                    if ( other instanceof NodeRecord )
                    {
                        report.multipleOwners( (NodeRecord) other );
                    }
                    else if ( other instanceof RelationshipRecord )
                    {
                        report.multipleOwners( (RelationshipRecord) other );
                    }
                }
            };

    PrimitiveRecordCheck( RecordField<RECORD, REPORT>... fields )
    {
        this.fields = Arrays.copyOf( fields, fields.length + 1 );
        this.fields[fields.length] = new FirstProperty();
    }

    abstract PropertyOwner owner( RECORD record );

    private class FirstProperty
            implements RecordField<RECORD, REPORT>, ComparativeRecordChecker<RECORD, PropertyRecord, REPORT>
    {
        @Override
        public void checkConsistency( RECORD record, REPORT report, RecordReferencer records )
        {
            if ( !Record.NO_NEXT_PROPERTY.is( record.getNextProp() ) )
            {
                report.forReference( records.property( record.getNextProp() ), this );
            }
        }

        @Override
        public void checkReference( RECORD record, PropertyRecord property, REPORT report )
        {
            if ( !property.inUse() )
            {
                report.propertyNotInUse( property );
            }
            else
            {
                if ( !Record.NO_PREVIOUS_PROPERTY.is( property.getPrevProp() ) )
                {
                    report.propertyNotFirstInChain( property );
                }
            }
        }
    }

    @Override
    public void check( RECORD record, REPORT report, RecordReferencer records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        for ( RecordField<RECORD, REPORT> field : fields )
        {
            field.checkConsistency( record, report, records );
        }
    }

    @Override
    public void checkChange( RECORD oldRecord, RECORD newRecord, REPORT report, RecordReferencer records )
    {
        check( newRecord, report, records );
    }
}
