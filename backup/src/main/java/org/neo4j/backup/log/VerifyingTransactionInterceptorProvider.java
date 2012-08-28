package org.neo4j.backup.log;

import org.neo4j.backup.consistency.check.incremental.IncrementalCheck;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.util.StringLogger;

@Service.Implementation(TransactionInterceptorProvider.class)
public class VerifyingTransactionInterceptorProvider extends CheckingTransactionInterceptorProvider
{
    public static final String NAME = "verifying";

    public VerifyingTransactionInterceptorProvider()
    {
        super( NAME );
    }

    @Override
    IncrementalCheck createChecker( String mode, StringLogger logger )
    {
        if ( !"true".equalsIgnoreCase( mode ) )
        {
            return null;
        }
        return new IncrementalCheck( logger );
    }
}
