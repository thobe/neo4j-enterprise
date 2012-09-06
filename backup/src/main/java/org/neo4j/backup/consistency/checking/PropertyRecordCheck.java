/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.backup.consistency.checking;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;

public class PropertyRecordCheck
        implements RecordCheck<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
{
    @Override
    public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                             ConsistencyReport.PropertyConsistencyReport report, DiffRecordReferencer records )
    {
        check( newRecord, report, records );
        if ( oldRecord.inUse() )
        {
            for ( RecordField<PropertyRecord, ConsistencyReport.PropertyConsistencyReport> field : PropertyField
                    .values() )
            {
                if ( field.valueFrom( newRecord ) != field.valueFrom( oldRecord ) && !field.isNone( oldRecord ) )
                {
                    if ( !field.referencedRecordChanged( records, oldRecord ) )
                    {
                        field.reportReplacedButNotUpdated( report );
                    }
                }
            }
        }
        Map<Long, PropertyBlock> prevStrings = new HashMap<Long, PropertyBlock>();
        Map<Long, PropertyBlock> prevArrays = new HashMap<Long, PropertyBlock>();
        for ( PropertyBlock block : oldRecord.getPropertyBlocks() )
        {
            PropertyType type = block.getType();
            if ( type != null )
            {
                switch ( type )
                {
                case STRING:
                    prevStrings.put( block.getSingleValueLong(), block );
                    break;
                case ARRAY:
                    prevArrays.put( block.getSingleValueLong(), block );
                    break;
                }
            }
        }
        for ( PropertyBlock block : newRecord.getPropertyBlocks() )
        {
            PropertyType type = block.getType();
            if ( type != null )
            {
                switch ( type )
                {
                case STRING:
                    prevStrings.remove( block.getSingleValueLong() );
                    break;
                case ARRAY:
                    prevArrays.remove( block.getSingleValueLong() );
                    break;
                }
            }
        }
        for ( PropertyBlock block : prevStrings.values() )
        {
            if ( records.changedString( block.getSingleValueLong() ) == null )
            {
                report.stringUnreferencedButNotDeleted( block );
            }
        }
        for ( PropertyBlock block : prevArrays.values() )
        {
            if ( records.changedArray( block.getSingleValueLong() ) == null )
            {
                report.arrayUnreferencedButNotDeleted( block );
            }
        }
    }

    @Override
    public void check( PropertyRecord record, ConsistencyReport.PropertyConsistencyReport report,
                       RecordReferencer records )
    {
        if ( !record.inUse() )
        {
            return;
        }
        for ( PropertyField field : PropertyField.values() )
        {
            field.checkConsistency( record, report, records );
        }
        for ( PropertyBlock block : record.getPropertyBlocks() )
        {
            checkDataBlock( block, report, records );
        }
    }

    private void checkDataBlock( PropertyBlock block, ConsistencyReport.PropertyConsistencyReport report,
                                 RecordReferencer records )
    {
        if ( block.getKeyIndexId() < 0 )
        {
            report.invalidPropertyKey( block );
        }
        else
        {
            report.forReference( records.propertyKey( block.getKeyIndexId() ), propertyKey( block ) );
        }
        PropertyType type = block.forceGetType();
        if ( type == null )
        {
            report.invalidPropertyType( block );
        }
        else
        {
            switch ( type )
            {
            case STRING:
                report.forReference( records.string( block.getSingleValueLong() ), DynamicReference.string( block ) );
                break;
            case ARRAY:
                report.forReference( records.array( block.getSingleValueLong() ), DynamicReference.array( block ) );
                break;
            default:
                try
                {
                    type.getValue( block, null );
                }
                catch ( Exception e )
                {
                    report.invalidPropertyValue( block );
                }
                break;
            }
        }
    }

    private enum PropertyField implements
            RecordField<PropertyRecord, ConsistencyReport.PropertyConsistencyReport>,
            ComparativeRecordChecker<PropertyRecord, PropertyRecord, ConsistencyReport.PropertyConsistencyReport>
    {
        PREV( Record.NO_PREVIOUS_PROPERTY )
        {
            @Override
            public long valueFrom( PropertyRecord record )
            {
                return record.getPrevProp();
            }

            @Override
            long otherReference( PropertyRecord record )
            {
                return record.getNextProp();
            }

            @Override
            void notInUse( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.previousNotInUse( property );
            }

            @Override
            void noBackReference( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.previousDoesNotReferenceBack( property );
            }

            @Override
            public void reportReplacedButNotUpdated( ConsistencyReport.PropertyConsistencyReport report )
            {
                report.previousReplacedButNotUpdated();
            }
        },
        NEXT( Record.NO_NEXT_PROPERTY )
        {
            @Override
            public long valueFrom( PropertyRecord record )
            {
                return record.getNextProp();
            }

            @Override
            long otherReference( PropertyRecord record )
            {
                return record.getPrevProp();
            }

            @Override
            void notInUse( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.nextNotInUse( property );
            }

            @Override
            void noBackReference( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property )
            {
                report.nextDoesNotReferenceBack( property );
            }

            @Override
            public void reportReplacedButNotUpdated( ConsistencyReport.PropertyConsistencyReport report )
            {
                report.nextReplacedButNotUpdated();
            }
        };
        private final Record NONE;

        private PropertyField( Record none )
        {
            this.NONE = none;
        }

        abstract long otherReference( PropertyRecord record );

        @Override
        public void checkConsistency( PropertyRecord record, ConsistencyReport.PropertyConsistencyReport report,
                                      RecordReferencer records )
        {
            if ( !NONE.is( valueFrom( record ) ) )
            {
                report.forReference( records.property( valueFrom( record ) ), this );
            }
        }

        @Override
        public boolean isNone( PropertyRecord record )
        {
            return NONE.is( valueFrom( record ) );
        }

        @Override
        public boolean referencedRecordChanged( DiffRecordReferencer records, PropertyRecord record )
        {
            return records.changedProperty( valueFrom( record ) ) != null;
        }

        @Override
        public void checkReference( PropertyRecord record, PropertyRecord referred,
                                    ConsistencyReport.PropertyConsistencyReport report )
        {
            if ( !referred.inUse() )
            {
                notInUse( report, referred );
            }
            else
            {
                if ( otherReference( referred ) != record.getId() )
                {
                    noBackReference( report, referred );
                }
            }
        }

        abstract void notInUse( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property );

        abstract void noBackReference( ConsistencyReport.PropertyConsistencyReport report, PropertyRecord property );
    }

    private static ComparativeRecordChecker<PropertyRecord, PropertyIndexRecord, ConsistencyReport.PropertyConsistencyReport>
    propertyKey( final PropertyBlock block )
    {
        return new ComparativeRecordChecker<PropertyRecord, PropertyIndexRecord, ConsistencyReport.PropertyConsistencyReport>()
        {
            @Override
            public void checkReference( PropertyRecord record, PropertyIndexRecord referred,
                                        ConsistencyReport.PropertyConsistencyReport report )
            {
                if ( !referred.inUse() )
                {
                    report.keyNotInUse( block, referred );
                }
            }
        };
    }

    private static abstract class DynamicReference implements
            ComparativeRecordChecker<PropertyRecord, DynamicRecord, ConsistencyReport.PropertyConsistencyReport>
    {
        final PropertyBlock block;

        private DynamicReference( PropertyBlock block )
        {
            this.block = block;
        }

        public static DynamicReference string( PropertyBlock block )
        {
            return new DynamicReference( block )
            {
                @Override
                void notUsed( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.stringNotInUse( block, value );
                }

                @Override
                void empty( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.stringEmpty( block, value );
                }
            };
        }

        public static DynamicReference array( PropertyBlock block )
        {
            return new DynamicReference( block )
            {
                @Override
                void notUsed( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.arrayNotInUse( block, value );
                }

                @Override
                void empty( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value )
                {
                    report.arrayEmpty( block, value );
                }
            };
        }

        @Override
        public void checkReference( PropertyRecord record, DynamicRecord referred,
                                    ConsistencyReport.PropertyConsistencyReport report )
        {
            if ( !referred.inUse() )
            {
                notUsed( report, referred );
            }
            else
            {
                if ( referred.getLength() <= 0 )
                {
                    empty( report, referred );
                }
            }
        }

        abstract void notUsed( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value );

        abstract void empty( ConsistencyReport.PropertyConsistencyReport report, DynamicRecord value );
    }
}