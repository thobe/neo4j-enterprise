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
package org.neo4j.backup.consistency.report;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
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
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.neo4j.backup.consistency.RecordType;
import org.neo4j.backup.consistency.checking.RecordCheck;
import org.neo4j.backup.consistency.store.DiffRecordReferencer;
import org.neo4j.backup.consistency.store.RecordAccess;
import org.neo4j.backup.consistency.store.RecordReferencer;
import org.neo4j.backup.consistency.store.ReferenceDispatcher;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.util.StringLogger;

import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
public class ConsistencyReporterTest implements Answer
{
    @Test
    public void shouldLogInconsistency() throws Exception
    {
        // given
        StringBuffer result = new StringBuffer();
        StringLogger logger = StringLogger.wrap( result );
        ConsistencyReport.Reporter reporter = ConsistencyReporter
                .create( mock( RecordAccess.class ), mock( ReferenceDispatcher.class ), logger );

        // when
        reportMethod.invoke( reporter, parameters( reportMethod ) );

        // then
        assertThat( result.toString(), containsString( " // " ) );
        // System.out.println( this + ": \n\t" + result );
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
            Type[] parameterTypes = reporterMethod.getGenericParameterTypes();
            ParameterizedType checkerParameter = (ParameterizedType) parameterTypes[parameterTypes.length - 1];
            Class reportType = (Class) checkerParameter.getActualTypeArguments()[1];
            for ( Method method : reportType.getMethods() )
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

        return String.format( "report.%s(%s{ reporter.%s(); })",
                              reportMethod.getName(), signatureOf( reportMethod ), method.getName() );
    }

    private static String signatureOf( Method reportMethod )
    {
        if ( reportMethod.getParameterTypes().length == 2 )
        {
            return "record, RecordCheck( reporter )";
        }
        else
        {
            return "oldRecord, newRecord, RecordCheck( reporter )";
        }
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
        if ( type == RecordType.class )
        {
            return RecordType.STRING_PROPERTY;
        }
        if ( type == RecordCheck.class )
        {
            return mockChecker();
        }
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

    @SuppressWarnings("unchecked")
    private RecordCheck mockChecker()
    {
        RecordCheck checker = mock( RecordCheck.class );
        doAnswer( this ).when( checker ).check( any( AbstractBaseRecord.class ),
                                                any( ConsistencyReport.class ),
                                                any( RecordReferencer.class ) );
        doAnswer( this ).when( checker ).checkChange( any( AbstractBaseRecord.class ),
                                                      any( AbstractBaseRecord.class ),
                                                      any( ConsistencyReport.class ),
                                                      any( DiffRecordReferencer.class ) );
        return checker;
    }

    @Override
    public Object answer( InvocationOnMock invocation ) throws Throwable
    {
        Object[] arguments = invocation.getArguments();
        return method.invoke( arguments[arguments.length-2], parameters( method ) );
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
