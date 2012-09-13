package org.neo4j.backup.consistency.checking;

import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.store.DiffRecordAccess;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;

public enum DynamicStore
{
    STRING( RecordType.STRING_PROPERTY )
            {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.string( block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return records.changedString( id );
        }
    },
    ARRAY( RecordType.ARRAY_PROPERTY )
            {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.array( block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return records.changedArray( id );
        }
    },
    PROPERTY_KEY( RecordType.PROPERTY_KEY_NAME )
            {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.propertyKeyName( (int) block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return null; // never needed
        }
    },
    RELATIONSHIP_LABEL( RecordType.RELATIONSHIP_LABEL_NAME )
            {
        @Override
        RecordReference<DynamicRecord> lookup( RecordAccess records, long block )
        {
            return records.relationshipLabelName( (int) block );
        }

        @Override
        DynamicRecord changed( DiffRecordAccess records, long id )
        {
            return null; // never needed
        }
    };
    public final RecordType type;

    private DynamicStore( RecordType type )
    {
        this.type = type;
    }

    abstract RecordReference<DynamicRecord> lookup(RecordAccess records, long block);

    abstract DynamicRecord changed( DiffRecordAccess records, long id );
}
