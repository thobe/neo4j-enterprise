package org.neo4j.backup.consistency.check;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PrimitiveRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;

public interface ConsistencyReport<RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
{
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.METHOD)
    public @interface Warning
    {
    }

    public interface Reporter
    {
        void forNode( NodeRecord node,
                      RecordCheck<NodeRecord, NodeConsistencyReport> checker );

        void forRelationship( RelationshipRecord relationship,
                              RecordCheck<RelationshipRecord, RelationshipConsistencyReport> checker );

        void forProperty( PropertyRecord property,
                          RecordCheck<PropertyRecord, PropertyConsistencyReport> checker );

        void forRelationshipLabel( RelationshipTypeRecord label,
                                   RecordCheck<RelationshipTypeRecord, LabelConsistencyReport> checker );

        void forPropertyKey( PropertyIndexRecord key,
                             RecordCheck<PropertyIndexRecord, PropertyKeyConsistencyReport> checker );

        void forDynamicBlock( RecordType type, DynamicRecord record,
                              RecordCheck<DynamicRecord, DynamicConsistencyReport> checker );
    }

    <REFERRED extends AbstractBaseRecord> void forReference( RecordReference<REFERRED> other,
                                                             ComparativeRecordChecker<RECORD, REFERRED, REPORT> checker );

    interface PrimitiveConsistencyReport<RECORD extends PrimitiveRecord, REPORT extends PrimitiveConsistencyReport<RECORD, REPORT>>
            extends ConsistencyReport<RECORD, REPORT>
    {
        /** The referenced property record is not in use. */
        @Documented
        void propertyNotInUse( PropertyRecord property );

        /** The referenced property record is not the first in its property chain. */
        @Documented
        void propertyNotFirstInChain( PropertyRecord property );

        /** The referenced property is owned by another Node. */
        @Documented
        void multipleOwners( NodeRecord node );

        /** The referenced property is owned by another Relationship. */
        @Documented
        void multipleOwners( RelationshipRecord relationship );
    }

    interface NodeConsistencyReport extends PrimitiveConsistencyReport<NodeRecord, NodeConsistencyReport>
    {
        /** The referenced relationship record is not in use. */
        @Documented
        void relationshipNotInUse( RelationshipRecord referenced );

        /** The referenced relationship record is a relationship between two other nodes. */
        @Documented
        void relationshipForOtherNode( RelationshipRecord relationship );

        /** The referenced relationship record is not the first in the relationship chain where this node is source. */
        @Documented
        void relationshipNotFirstInSourceChain( RelationshipRecord relationship );

        /** The referenced relationship record is not the first in the relationship chain where this node is target. */
        @Documented
        void relationshipNotFirstInTargetChain( RelationshipRecord relationship );
    }

    interface RelationshipConsistencyReport
            extends PrimitiveConsistencyReport<RelationshipRecord, RelationshipConsistencyReport>
    {
        /** The label field has an illegal value. */
        @Documented
        void illegalLabel();

        /** The label record is not in use. */
        @Documented
        void labelNotInUse( RelationshipTypeRecord label );

        /** The source node field has an illegal value. */
        @Documented
        void illegalSourceNode();

        /** The target node field has an illegal value. */
        @Documented
        void illegalTargetNode();

        /** The source node is not in use. */
        @Documented
        void sourceNodeNotInUse( NodeRecord node );

        /** The target node is not in use. */
        @Documented
        void targetNodeNotInUse( NodeRecord node );

        /** This record should be the first in the source chain, but the source node does not reference this record. */
        @Documented
        void sourceNodeDoesNotReferenceBack( NodeRecord node );

        /** This record should be the first in the target chain, but the target node does not reference this record. */
        @Documented
        void targetNodeDoesNotReferenceBack( NodeRecord node );

        /** The source node does not have a relationship chain. */
        @Documented
        void sourceNodeHasNoRelationships( NodeRecord source );

        /** The target node does not have a relationship chain. */
        @Documented
        void targetNodeHasNoRelationships( NodeRecord source );

        /** The previous record in the source chain is a relationship between two other nodes. */
        @Documented
        void sourcePrevReferencesOtherNodes( RelationshipRecord relationship );

        /** The next record in the source chain is a relationship between two other nodes. */
        @Documented
        void sourceNextReferencesOtherNodes( RelationshipRecord relationship );

        /** The previous record in the target chain is a relationship between two other nodes. */
        @Documented
        void targetPrevReferencesOtherNodes( RelationshipRecord relationship );

        /** The next record in the target chain is a relationship between two other nodes. */
        @Documented
        void targetNextReferencesOtherNodes( RelationshipRecord relationship );

        /** The previous record in the source chain does not have this record as its next record. */
        @Documented
        void sourcePrevDoesNotReferenceBack( RelationshipRecord relationship );

        /** The next record in the source chain does not have this record as its previous record. */
        @Documented
        void sourceNextDoesNotReferenceBack( RelationshipRecord relationship );

        /** The previous record in the target chain does not have this record as its next record. */
        @Documented
        void targetPrevDoesNotReferenceBack( RelationshipRecord relationship );

        /** The next record in the target chain does not have this record as its previous record. */
        @Documented
        void targetNextDoesNotReferenceBack( RelationshipRecord relationship );
    }

    interface PropertyConsistencyReport extends ConsistencyReport<PropertyRecord, PropertyConsistencyReport>
    {
        /** The property key as an invalid value. */
        @Documented
        void invalidPropertyKey( PropertyBlock block );

        /** The key for this property is not in use. */
        @Documented
        void keyNotInUse( PropertyBlock block, PropertyIndexRecord key );

        /** The previous property record is not in use. */
        @Documented
        void previousNotInUse( PropertyRecord property );

        /** The next property record is not in use. */
        @Documented
        void nextNotInUse( PropertyRecord property );

        /** The previous property record does not have this record as its next record. */
        @Documented
        void previousDoesNotReferenceBack( PropertyRecord property );

        /** The next property record does not have this record as its previous record. */
        @Documented
        void nextDoesNotReferenceBack( PropertyRecord property );

        /** The type of this property is invalid. */
        @Documented
        void invalidPropertyType( PropertyBlock block );

        /** The string block is not in use. */
        @Documented
        void stringNotInUse( PropertyBlock block, DynamicRecord value );

        /** The array block is not in use. */
        @Documented
        void arrayNotInUse( PropertyBlock block, DynamicRecord value );

        /** The string block is empty. */
        @Documented
        void stringEmpty( PropertyBlock block, DynamicRecord value );

        /** The array block is empty. */
        @Documented
        void arrayEmpty( PropertyBlock block, DynamicRecord value );

        /** The property value is invalid. */
        @Documented
        void invalidPropertyValue( PropertyBlock block );
    }

    interface LabelConsistencyReport extends ConsistencyReport<RelationshipTypeRecord, LabelConsistencyReport>
    {
        /** The name block is not in use. */
        @Documented
        void nameBlockNotInUse( DynamicRecord record );

        /** The name is empty. */
        @Documented
        @Warning
        void emptyName( DynamicRecord name );
    }

    interface PropertyKeyConsistencyReport extends ConsistencyReport<PropertyIndexRecord, PropertyKeyConsistencyReport>
    {
        /** The name block is not in use. */
        @Documented
        void nameBlockNotInUse( DynamicRecord name );

        /** The name is empty. */
        @Documented
        @Warning
        void emptyName( DynamicRecord name );
    }

    interface DynamicConsistencyReport extends ConsistencyReport<DynamicRecord, DynamicConsistencyReport>
    {
        /** The next block is not in use. */
        @Documented
        void nextNotInUse( DynamicRecord next );

        /** The record is not full, but references a next block. */
        @Documented
        @Warning
        void recordNotFullReferencesNext();

        /** The length of the block is invalid. */
        @Documented
        void invalidLength();

        /** The block is empty. */
        @Documented
        @Warning
        void emptyBlock();

        /** The next block is empty. */
        @Documented
        @Warning
        void emptyNextBlock( DynamicRecord next );

        /** The next block references this (the same) record. */
        @Documented
        void selfReferentialNext();
    }
}
