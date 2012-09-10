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

import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.backup.consistency.store.RecordAccessStub;
import org.neo4j.backup.consistency.store.RecordReference;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public abstract class RecordCheckTestBase<RECORD extends AbstractBaseRecord,
        REPORT extends ConsistencyReport<RECORD, REPORT>,
        CHECKER extends RecordCheck<RECORD, REPORT>>
{
    public static final int NONE = -1;
    private final CHECKER checker;
    private final Class<REPORT> reportClass;
    private final RecordAccessStub records = new RecordAccessStub();

    RecordCheckTestBase( CHECKER checker, Class<REPORT> reportClass )
    {
        this.checker = checker;
        this.reportClass = reportClass;
    }

    final REPORT check( RECORD record )
    {
        return check( reportClass, checker, record, records );
    }

    final REPORT checkChange( RECORD oldRecord, RECORD newRecord )
    {
        return checkChange( reportClass, checker, oldRecord, newRecord, records );
    }

    public static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    REPORT check( Class<REPORT> reportClass, RecordCheck<RECORD, REPORT> checker, RECORD record,
                  final RecordAccessStub records )
    {
        REPORT report = records.mockReport( reportClass, record );
        checker.check( record, report, records );
        records.checkDeferred();
        return report;
    }

    static <RECORD extends AbstractBaseRecord, REPORT extends ConsistencyReport<RECORD, REPORT>>
    REPORT checkChange( Class<REPORT> reportClass, RecordCheck<RECORD, REPORT> checker,
                        RECORD oldRecord, RECORD newRecord, final RecordAccessStub records )
    {
        REPORT report = records.mockReport( reportClass, oldRecord, newRecord );
        checker.checkChange( oldRecord, newRecord, report, records );
        records.checkDeferred();
        return report;
    }

    <R extends AbstractBaseRecord> R addChange( R oldRecord, R newRecord )
    {
        return records.addChange( oldRecord, newRecord );
    }

    <R extends AbstractBaseRecord> R add( R record )
    {
        return records.add( record );
    }

    DynamicRecord addKeyName( DynamicRecord name )
    {
        return records.addKeyName( name );
    }

    DynamicRecord addLabelName( DynamicRecord name )
    {
        return records.addLabelName( name );
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
    public static void verifyOnlyReferenceDispatch( ConsistencyReport report )
    {
        verify( report, atLeast( 0 ) )
                .forReference( any( RecordReference.class ), any( ComparativeRecordChecker.class ) );
        verifyNoMoreInteractions( report );
    }
}
