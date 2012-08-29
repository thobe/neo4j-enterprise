package org.neo4j.backup.log;

import org.neo4j.backup.consistency.check.InconsistentStoreException;
import org.neo4j.backup.consistency.check.incremental.DiffCheck;
import org.neo4j.backup.consistency.check.incremental.FullDiffCheck;
import org.neo4j.backup.consistency.check.incremental.IncrementalDiffCheck;
import org.neo4j.backup.consistency.store.DiffStore;
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
        FULL
        {
            @Override
            DiffCheck createChecker( StringLogger logger )
            {
                return new FullDiffCheck( logger );
            }
        },
        DIFF
        {
            @Override
            DiffCheck createChecker( StringLogger logger )
            {
                return new IncrementalDiffCheck( logger );
            }
        };

        abstract DiffCheck createChecker( StringLogger logger );
    }

    @Override
    DiffCheck createChecker( String mode, StringLogger logger )
    {
        final CheckerMode checkerMode;
        try
        {
            checkerMode = CheckerMode.valueOf( mode.toUpperCase() );
        }
        catch ( Exception e )
        {
            return null;
        }
        return new LoggingDiffCheck( checkerMode.createChecker( logger ), logger );
    }

    private static class LoggingDiffCheck implements DiffCheck
    {
        private final DiffCheck checker;
        private final StringLogger logger;

        public LoggingDiffCheck( DiffCheck checker, StringLogger logger )
        {
            this.checker = checker;
            this.logger = logger;
        }

        @Override
        public void check( DiffStore diffs )
        {
            try
            {
                checker.check( diffs );
            }
            catch ( InconsistentStoreException e )
            {
                logger.logMessage( e.getMessage() );
            }
        }
    }
}
