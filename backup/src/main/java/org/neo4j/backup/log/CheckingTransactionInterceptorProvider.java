package org.neo4j.backup.log;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.backup.consistency.checking.incremental.DiffCheck;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.transaction.xaframework.XaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;

abstract class CheckingTransactionInterceptorProvider extends TransactionInterceptorProvider
{
    public CheckingTransactionInterceptorProvider( String name )
    {
        super( name );
    }

    abstract DiffCheck createChecker( String mode, StringLogger logger );

    @Override
    public CheckingTransactionInterceptor create( XaDataSource ds, Object options,
                                                  DependencyResolver dependencyResolver )
    {
        if ( !(ds instanceof NeoStoreXaDataSource) || !(options instanceof String) )
        {
            return null;
        }
        String[] config = ((String) options).split( ";" );
        String mode = config[0];
        Map<String, String> parameters = new HashMap<String, String>();
        for ( int i = 1; i < config.length; i++ )
        {
            String[] parts = config[i].split( "=" );
            parameters.put( parts[0].toLowerCase(), parts.length == 1 ? "true" : parts[1] );
        }
        StringLogger logger = dependencyResolver.resolveDependency( StringLogger.class );
        DiffCheck check = createChecker( mode, logger );
        if ( check == null )
        {
            return null;
        }
        else
        {
            String log = parameters.get( "log" );
            return new CheckingTransactionInterceptor( check, (NeoStoreXaDataSource) ds, logger, log );
        }
    }

    @Override
    public CheckingTransactionInterceptor create( TransactionInterceptor next, XaDataSource ds, Object options,
                                                  DependencyResolver dependencyResolver )
    {
        CheckingTransactionInterceptor interceptor = create( ds, options, dependencyResolver );
        if ( interceptor != null )
        {
            interceptor.setNext( next );
        }
        return interceptor;
    }
}
