package org.neo4j.backup.log;

import java.io.File;

import org.neo4j.backup.consistency.ConsistencyCheckingError;
import org.neo4j.backup.consistency.check.InconsistentStoreException;
import org.neo4j.backup.consistency.check.incremental.IncrementalCheck;
import org.neo4j.backup.consistency.store.DiffStore;
import org.neo4j.kernel.impl.nioneo.store.NeoStoreRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyIndexRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeRecord;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptor;
import org.neo4j.kernel.impl.util.StringLogger;

class CheckingTransactionInterceptor implements TransactionInterceptor
{
    private TransactionInterceptor next;
    private LogEntry.Start startEntry;
    private LogEntry.Commit commitEntry;
    private final DiffStore diffs;
    private final IncrementalCheck checker;
    private final StringLogger diffLog;

    CheckingTransactionInterceptor( IncrementalCheck checker, NeoStoreXaDataSource dataSource,
                                    StringLogger logger, String log )
    {
        StringLogger diffLog = null;
        if ( log != null )
        {
            if ( "true".equalsIgnoreCase( log ) )
            {
                diffLog = logger;
            }
            else
            {
                diffLog = StringLogger.logger( new File( log ) );
            }
        }
        this.checker = checker;
        this.diffLog = diffLog;
        this.diffs = new DiffStore( dataSource.getNeoStore() );
    }

    void setNext( TransactionInterceptor next )
    {
        this.next = next;
    }

    @Override
    public void setStartEntry( LogEntry.Start startEntry )
    {
        this.startEntry = startEntry;
        if ( next != null )
        {
            next.setStartEntry( startEntry );
        }
    }

    @Override
    public void setCommitEntry( LogEntry.Commit commitEntry )
    {
        this.commitEntry = commitEntry;
        if ( next != null )
        {
            next.setCommitEntry( commitEntry );
        }
    }

    @Override
    public void visitNode( NodeRecord record )
    {
        diffs.visitNode( record );
        if ( next != null )
        {
            next.visitNode( record );
        }
    }

    @Override
    public void visitRelationship( RelationshipRecord record )
    {
        diffs.visitRelationship( record );
        if ( next != null )
        {
            next.visitRelationship( record );
        }
    }

    @Override
    public void visitProperty( PropertyRecord record )
    {
        diffs.visitProperty( record );
        if ( next != null )
        {
            next.visitProperty( record );
        }
    }

    @Override
    public void visitRelationshipType( RelationshipTypeRecord record )
    {
        diffs.visitRelationshipType( record );
        if ( next != null )
        {
            next.visitRelationshipType( record );
        }
    }

    @Override
    public void visitPropertyIndex( PropertyIndexRecord record )
    {
        diffs.visitPropertyIndex( record );
        if ( next != null )
        {
            next.visitPropertyIndex( record );
        }
    }

    @Override
    public void visitNeoStore( NeoStoreRecord record )
    {
        diffs.visitNeoStore( record );
        if ( next != null )
        {
            next.visitNeoStore( record );
        }
    }

    @Override
    public void complete() throws ConsistencyCheckingError
    {
        // TODO: move the logging code from VerifyingTransactionInterceptor to this class, then remove that class
        try
        {
            checker.check( diffs );
        }
        catch ( InconsistentStoreException inconsistency )
        {
            throw new ConsistencyCheckingError( startEntry, commitEntry, inconsistency.summary() );
        }
    }
}
