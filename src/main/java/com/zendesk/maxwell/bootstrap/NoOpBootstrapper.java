package com.zendesk.maxwell.bootstrap;

import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.replication.Replicator;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.producer.AbstractProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoOpBootstrapper extends AbstractBootstrapper {
	public NoOpBootstrapper(MaxwellContext context) { super( context ); }

	@Override
	public boolean shouldSkip(RowMap row) {
		return false;
	}

	@Override
	public void resume(AbstractProducer producer, Replicator replicator) throws Exception { }

	@Override
	public boolean isRunning( ) {
		return false;
	}

	@Override
	public void work(RowMap row, AbstractProducer producer, Replicator replicator) throws Exception {}

}
