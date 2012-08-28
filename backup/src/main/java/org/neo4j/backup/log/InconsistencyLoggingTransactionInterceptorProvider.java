package org.neo4j.backup.log;

import org.neo4j.backup.consistency.check.incremental.IncrementalCheck;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.util.StringLogger;

@Service.Implementation(TransactionInterceptorProvider.class)
public class InconsistencyLoggingTransactionInterceptorProvider extends CheckingTransactionInterceptorProvider
{
    public static final String NAME = "inconsistency" + "log";

    public InconsistencyLoggingTransactionInterceptorProvider()
    {
        super( NAME );
    }

    public enum CheckerMode
    {
        FULL, DIFF
    }

    @Override
    IncrementalCheck createChecker( String mode, StringLogger logger )
    {
        // TODO: implement this
        final CheckerMode checkerMode;
        try
        {
            checkerMode = CheckerMode.valueOf( mode.toUpperCase() );
        }
        catch ( Exception e )
        {
            return null;
        }
        return null;
    }
}
