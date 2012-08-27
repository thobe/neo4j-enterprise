package org.neo4j.backup.consistency.report;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.model.Statement;
import org.neo4j.backup.consistency.check.ConsistencyReport;
import org.neo4j.backup.consistency.store.ReferenceDispatcher;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class ConsistencyReporterTest
{
    @Test
    public void shouldLogInconsistency() throws Exception
    {
        // given
        StringBuffer result = new StringBuffer();
        StringLogger logger = StringLogger.wrap( result );
        ConsistencyReport.Reporter reporter = ConsistencyReporter
                .create( mock( ReferenceDispatcher.class ), logger );
        Object report = reportMethod.invoke( reporter, parameters( reportMethod ) );

        // when
        method.invoke( report, parameters( method ) );

        // then
        assertThat( result.toString(), containsString( " // " ) );
        System.out.println( result );
    }

    private final Method reportMethod;
    private final Method method;

    public ConsistencyReporterTest( Method reportMethod, Method method )
    {
        this.reportMethod = reportMethod;
        this.method = method;
    }

    @Parameterized.Parameters
    public static List<Object[]> methods()
    {
        ArrayList<Object[]> methods = new ArrayList<Object[]>();
        for ( Method reporterMethod : ConsistencyReport.Reporter.class.getMethods() )
        {
            for ( Method method : reporterMethod.getReturnType().getMethods() )
            {
                if ( !method.getName().equals( "forReference" ) )
                {
                    methods.add( new Object[]{reporterMethod, method} );
                }
            }
        }
        return methods;
    }

    @Rule
    public final TestRule logFailure = new TestRule()
    {
        @Override
        public Statement apply( final Statement base, org.junit.runner.Description description )
        {
            return new Statement()
            {
                @Override
                public void evaluate() throws Throwable
                {
                    try
                    {
                        base.evaluate();
                    }
                    catch ( Throwable failure )
                    {
                        System.err.println( "Failure in " + ConsistencyReporterTest.this + ": " + failure );
                        throw failure;
                    }
                }
            };
        }
    };

    @Override
    public String toString()
    {
        return "report." + reportMethod.getName() + "()." + method.getName() + "()";
    }

    private Object[] parameters( Method method )
    {
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] parameters = new Object[parameterTypes.length];
        for ( int i = 0; i < parameters.length; i++ )
        {
            parameters[i] = parameter( parameterTypes[i] );
        }
        return parameters;
    }

    private Object parameter( Class<?> type )
    {
        if ( type == NodeRecord.class )
        {
            return new NodeRecord( 0, 1, 2 );
        }
        if ( type == RelationshipRecord.class )
        {
            return new RelationshipRecord( 0, 1, 2, 3 );
        }
        if ( type == PropertyRecord.class )
        {
            return new PropertyRecord( 0 );
        }
        if ( type == PropertyIndexRecord.class )
        {
            return new PropertyIndexRecord( 0 );
        }
        if ( type == PropertyBlock.class )
        {
            return new PropertyBlock();
        }
        if ( type == RelationshipTypeRecord.class )
        {
            return new RelationshipTypeRecord( 0 );
        }
        if ( type == DynamicRecord.class )
        {
            return new DynamicRecord( 0 );
        }
        throw new IllegalArgumentException( type.getName() );
    }

    private static Matcher<String> containsString( final String substring )
    {
        return new TypeSafeMatcher<String>()
        {
            @Override
            public boolean matchesSafely( String item )
            {
                return item.contains( substring );
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "String containing substring " ).appendValue( substring );
            }
        };
    }
}
