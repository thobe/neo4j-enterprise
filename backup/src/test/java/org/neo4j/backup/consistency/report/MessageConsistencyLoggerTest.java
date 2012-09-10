package org.neo4j.backup.consistency.report;

import java.io.StringWriter;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;
import org.neo4j.backup.consistency.RecordType;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.assertThat;

public class MessageConsistencyLoggerTest
{
    // given
    private final MessageConsistencyLogger logger;
    private final StringWriter writer;

    {
        writer = new StringWriter();
        logger = new MessageConsistencyLogger( StringLogger.wrap( writer ) );
    }

    @Test
    public void shouldFormatErrorForRecord() throws Exception
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        assertTextEquals( "ERROR: sample message",
                          "NeoStoreRecord[used=true,nextProp=-1]",
                          "Inconsistent with: 1 2" );
    }

    @Test
    public void shouldFormatWarningForRecord() throws Exception
    {
        // when
        logger.warning( RecordType.NEO_STORE, new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        assertTextEquals( "WARNING: sample message",
                          "NeoStoreRecord[used=true,nextProp=-1]",
                          "Inconsistent with: 1 2" );
    }

    @Test
    public void shouldFormatErrorForChangedRecord() throws Exception
    {
        // when
        logger.error( RecordType.NEO_STORE, new NeoStoreRecord(), new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        assertTextEquals( "ERROR: sample message",
                          "- NeoStoreRecord[used=true,nextProp=-1]",
                          "+ NeoStoreRecord[used=true,nextProp=-1]",
                          "Inconsistent with: 1 2" );
    }

    @Test
    public void shouldFormatWarningForChangedRecord() throws Exception
    {
        // when
        logger.warning( RecordType.NEO_STORE, new NeoStoreRecord(), new NeoStoreRecord(), "sample message", 1, 2 );

        // then
        assertTextEquals( "WARNING: sample message",
                          "- NeoStoreRecord[used=true,nextProp=-1]",
                          "+ NeoStoreRecord[used=true,nextProp=-1]",
                          "Inconsistent with: 1 2" );
    }

    private void assertTextEquals( String firstLine, String... lines )
    {
        StringBuilder expected = new StringBuilder( firstLine );
        for ( String line : lines )
        {
            expected.append( "\n\t" ).append( line );
        }
        assertThat( writer.toString(), endsWith( expected.append( '\n' ).toString() ) );
    }

    private static Matcher<String> endsWith( final String suffix )
    {
        return new TypeSafeMatcher<String>()
        {
            @Override
            public boolean matchesSafely( String item )
            {
                return item.endsWith( suffix );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "String ending with " ).appendValue( suffix );
            }
        };
    }
}
