package org.neo4j.backup.consistency.check;

import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;

public class PropertyKeyRecordCheck
        extends NameRecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport>
{
    @Override
    protected RecordReference<DynamicRecord> name( RecordReferencer records, int id )
    {
        return records.propertyKeyName( id );
    }

    @Override
    void nameNotInUse( ConsistencyReport.PropertyKeyConsistencyReport report, DynamicRecord name )
    {
        report.nameBlockNotInUse( name );
    }

    @Override
    void emptyName( ConsistencyReport.PropertyKeyConsistencyReport report, DynamicRecord name )
    {
        report.emptyName( name );
    }
}
