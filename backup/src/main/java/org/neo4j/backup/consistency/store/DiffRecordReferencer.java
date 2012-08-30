package org.neo4j.backup.consistency.store;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;

/**
 * A {@link RecordReferencer} for use with incremental checking. Provides access to
 * <p/>
 * {@link #previousNode(long) Node}, {@link #previousRelationship(long) Relationship}, and {@link
 * #previousProperty(long) Property} are the only record types one might need a previous version of when checking
 * another type of record.
 * <p/>
 * Getting the new version of a record is an operation that can always be performed without any I/O, therefore these
 * return the records immediately, instead of returning {@link RecordReference} objects. New versions of records can be
 * retrieved for {@link #changedNode(long)} Node}, {@link #changedRelationship(long)} (long) Relationship}, {@link
 * #changedProperty(long)} (long) Property}, {@link #changedString(long) String property blocks}, and {@link
 * #changedArray(long) Array property blocks}, these are the only types of records for which there is a need to get the
 * new version while checking another type of record. The methods returning new versions of records return
 * <code>null</code> if the record has not been changed.
 */
public class DiffRecordReferencer extends RecordReferencer
{
    public DiffRecordReferencer( RecordAccess access )
    {
        super( access );
    }

    public RecordReference<NodeRecord> previousNode( long id )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    public RecordReference<RelationshipRecord> previousRelationship( long id )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    public RecordReference<PropertyRecord> previousProperty( long id )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    public NodeRecord changedNode( long id )
    {
        return access.changedNode( id );
    }

    public RelationshipRecord changedRelationship( long id )
    {
        return access.changedRelationship( id );
    }

    public PropertyRecord changedProperty( long id )
    {
        return access.changedProperty( id );
    }

    public DynamicRecord changedString( long id )
    {
        return access.changedString( id );
    }

    public DynamicRecord changedArray( long id )
    {
        return access.changedArray( id );
    }
}
