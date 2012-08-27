package org.neo4j.backup.consistency.check;

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
    public ConsistencyReport.PropertyConsistencyReport report( ConsistencyReport.Reporter reporter,
                                                               PropertyRecord property )
    {
        return reporter.forProperty( property );
    }

    @Override
    public void checkChange( PropertyRecord oldRecord, PropertyRecord newRecord,
                             ConsistencyReport.PropertyConsistencyReport report, RecordReferencer records )
    {
        check( newRecord, report, records );
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
            long valueFrom( PropertyRecord record )
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
        },
        NEXT( Record.NO_NEXT_PROPERTY )
        {
            @Override
            long valueFrom( PropertyRecord record )
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
        };
        private final Record NONE;

        private PropertyField( Record none )
        {
            this.NONE = none;
        }

        abstract long valueFrom( PropertyRecord record );

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
