package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public class RelationshipLabelRecordCheck
    extends NameRecordCheck<RelationshipTypeRecord,ConsistencyReport.LabelConsistencyReport>
{
    @Override
    protected RecordReference<DynamicRecord> name( RecordReferencer records, int id )
    {
        return records.relationshipLabelName( id );
    }

    @Override
    void nameNotInUse( ConsistencyReport.LabelConsistencyReport report, DynamicRecord name )
    {
        report.nameBlockNotInUse( name );
    }

    @Override
    void emptyName( ConsistencyReport.LabelConsistencyReport report, DynamicRecord name )
    {
        report.emptyName( name );
    }
}
