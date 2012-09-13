package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public interface CheckDecorator
{
    RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
            PrimitiveRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> checker );

    RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
            PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker );

    RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
            PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker );

    RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
            RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker );

    RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> decoratePropertyKeyChecker(
            RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker );

    RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> decorateLabelChecker(
            RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker );

    static CheckDecorator NONE = new CheckDecorator()
    {
        @Override
        public RecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> decorateNeoStoreChecker(
                PrimitiveRecordCheck<NeoStoreRecord, ConsistencyReport.NeoStoreConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> decorateNodeChecker(
                PrimitiveRecordCheck<NodeRecord, ConsistencyReport.NodeConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> decorateRelationshipChecker(
                PrimitiveRecordCheck<RelationshipRecord, ConsistencyReport.RelationshipConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> decoratePropertyChecker(
                RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> decoratePropertyKeyChecker(
                RecordCheck<PropertyIndexRecord, ConsistencyReport.PropertyKeyConsistencyReport> checker )
        {
            return checker;
        }

        @Override
        public RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> decorateLabelChecker(
                RecordCheck<RelationshipTypeRecord, ConsistencyReport.LabelConsistencyReport> checker )
        {
            return checker;
        }
    };
}
