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

import org.junit.Test;
import org.neo4j.backup.consistency.report.ConsistencyReport;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;

import static org.mockito.Mockito.verify;

public class PropertyRecordCheckTest
        extends RecordCheckTestBase<PropertyRecord, ConsistencyReport.PropertyConsistencyReport, PropertyRecordCheck>
{
    public PropertyRecordCheckTest()
    {
        super( new PropertyRecordCheck(), ConsistencyReport.PropertyConsistencyReport.class );
    }

    @Test
    public void shouldNotReportAnythingForPropertyRecordNotInUse() throws Exception
    {
        // given
        PropertyRecord property = notInUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldNotReportAnythingForPropertyWithoutBlocksThatDoesNotReferenceAnyOtherRecords() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPropertyKeyNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( notInUse( new PropertyIndexRecord( 0 ) ) );
        PropertyBlock block = propertyBlock( key, PropertyType.INT, 0 );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).keyNotInUse( block, key );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPreviousPropertyNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord prev = add( notInUse( new PropertyRecord( 51 ) ) );
        property.setPrevProp( prev.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).previousNotInUse( prev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportNextPropertyNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord next = add( notInUse( new PropertyRecord( 51 ) ) );
        property.setNextProp( next.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).nextNotInUse( next );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPreviousPropertyNotReferringBack() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord prev = add( inUse( new PropertyRecord( 51 ) ) );
        property.setPrevProp( prev.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).previousDoesNotReferenceBack( prev );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportNextPropertyNotReferringBack() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyRecord next = add( inUse( new PropertyRecord( 51 ) ) );
        property.setNextProp( next.getId() );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).nextDoesNotReferenceBack( next );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportStringRecordNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( notInUse( string( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );
        // then
        verify( report ).stringNotInUse( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportArrayRecordNotInUse() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( notInUse( array( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).arrayNotInUse( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyStringRecord() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( inUse( string( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).stringEmpty( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportEmptyArrayRecord() throws Exception
    {
        // given
        PropertyRecord property = inUse( new PropertyRecord( 42 ) );
        PropertyIndexRecord key = add( inUse( new PropertyIndexRecord( 6 ) ) );
        DynamicRecord value = add( inUse( array( new DynamicRecord( 1001 ) ) ) );
        PropertyBlock block = propertyBlock( key, value );
        property.addPropertyBlock( block );

        // when
        ConsistencyReport.PropertyConsistencyReport report = check( property );

        // then
        verify( report ).arrayEmpty( block, value );
        verifyOnlyReferenceDispatch( report );
    }

    // change checking

    @Test
    public void shouldNotReportAnythingForConsistentlyChangedProperty() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        oldProperty.setPrevProp( 1 );
        oldProperty.setNextProp( 2 );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        newProperty.setPrevProp( 11 );
        newProperty.setNextProp( 12 );

        addChange( inUse( new PropertyRecord( 1 ) ),
                   notInUse( new PropertyRecord( 1 ) ) );
        addChange( inUse( new PropertyRecord( 2 ) ),
                   notInUse( new PropertyRecord( 2 ) ) );

        addChange( notInUse( new PropertyRecord( 11 ) ),
                   inUse( new PropertyRecord( 11 ) ) ).setNextProp( 42 );
        addChange( notInUse( new PropertyRecord( 12 ) ),
                   inUse( new PropertyRecord( 12 ) ) ).setPrevProp( 42 );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportPreviousReplacedButNotUpdated() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        oldProperty.setPrevProp( 1 );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        newProperty.setPrevProp( 2 );

        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) ).setNextProp( 42 );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).previousReplacedButNotUpdated();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportNextReplacedButNotUpdated() throws Exception
    {
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        oldProperty.setNextProp( 1 );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );
        newProperty.setNextProp( 2 );

        addChange( notInUse( new PropertyRecord( 2 ) ),
                   inUse( new PropertyRecord( 2 ) ) ).setPrevProp( 42 );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).nextReplacedButNotUpdated();
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportStringValueUnreferencedButStillInUse() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyBlock block = propertyBlock( add( inUse( new PropertyIndexRecord( 1 ) ) ),
                                             add( string( inUse( new DynamicRecord( 100 ) ) ) ) );
        oldProperty.addPropertyBlock( block );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).stringUnreferencedButNotDeleted( block );
        verifyOnlyReferenceDispatch( report );
    }

    @Test
    public void shouldReportArrayValueUnreferencedButStillInUse() throws Exception
    {
        // given
        PropertyRecord oldProperty = inUse( new PropertyRecord( 42 ) );
        PropertyBlock block = propertyBlock( add( inUse( new PropertyIndexRecord( 1 ) ) ),
                                             add( array( inUse( new DynamicRecord( 100 ) ) ) ) );
        oldProperty.addPropertyBlock( block );
        PropertyRecord newProperty = inUse( new PropertyRecord( 42 ) );

        // when
        ConsistencyReport.PropertyConsistencyReport report = checkChange( oldProperty, newProperty );

        // then
        verify( report ).arrayUnreferencedButNotDeleted( block );
        verifyOnlyReferenceDispatch( report );
    }
}
