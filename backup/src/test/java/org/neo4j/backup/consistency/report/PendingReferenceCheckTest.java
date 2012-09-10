package org.neo4j.backup.consistency.report;

import java.lang.reflect.Proxy;

import org.junit.Test;
import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.checking.ComparativeRecordChecker;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class PendingReferenceCheckTest
{
    // given
    {
        ConsistencyReporter.ReportHandler handler =
                new ConsistencyReporter.ReportHandler(
                        StringLogger.DEV_NULL,
                        mock( ConsistencySummaryStatistics.class ),
                        RecordType.PROPERTY,
                        new PropertyRecord( 0 ) );
        ConsistencyReport.PropertyConsistencyReport report =
                (ConsistencyReport.PropertyConsistencyReport) Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class[]{ConsistencyReport.PropertyConsistencyReport.class},
                        handler );
        this.referenceCheck = new PendingReferenceCheck<PropertyRecord>( report, mock( ComparativeRecordChecker.class ) );
    }

    private final PendingReferenceCheck<PropertyRecord> referenceCheck;

    @Test
    public void shouldAllowSkipAfterSkip() throws Exception
    {
        // given
        referenceCheck.skip();
        // when
        referenceCheck.skip();
    }

    @Test
    public void shouldAllowSkipAfterCheckReference() throws Exception
    {
        // given
        referenceCheck.checkReference( new PropertyRecord( 0 ), null );
        // when
        referenceCheck.skip();
    }

    @Test
    public void shouldAllowSkipAfterCheckDiffReference() throws Exception
    {
        // given
        referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );
        // when
        referenceCheck.skip();
    }

    @Test
    public void shouldNotAllowCheckReferenceAfterSkip() throws Exception
    {
        // given
        referenceCheck.skip();

        // when
        try
        {
            referenceCheck.checkReference( new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckDiffReferenceAfterSkip() throws Exception
    {
        // given
        referenceCheck.skip();

        // when
        try
        {
            referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckReferenceAfterCheckReference() throws Exception
    {
        // given
        referenceCheck.checkReference( new PropertyRecord( 0 ), null );

        // when
        try
        {
            referenceCheck.checkReference( new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckDiffReferenceAfterCheckReference() throws Exception
    {
        // given
        referenceCheck.checkReference( new PropertyRecord( 0 ), null );

        // when
        try
        {
            referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckReferenceAfterCheckDiffReference() throws Exception
    {
        // given
        referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );

        // when
        try
        {
            referenceCheck.checkReference( new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }

    @Test
    public void shouldNotAllowCheckDiffReferenceAfterCheckDiffReference() throws Exception
    {
        // given
        referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );

        // when
        try
        {
            referenceCheck.checkDiffReference( new PropertyRecord( 0 ), new PropertyRecord( 0 ), null );
            fail( "expected exception" );
        }
        // then
        catch ( IllegalStateException expected )
        {
            assertEquals( "Reference has already been checked.", expected.getMessage() );
        }
    }
}
