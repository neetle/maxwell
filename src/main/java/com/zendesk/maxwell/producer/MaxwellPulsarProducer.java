package com.zendesk.maxwell.producer;

import com.zendesk.maxwell.MaxwellConfig;
import com.zendesk.maxwell.MaxwellContext;
import com.zendesk.maxwell.row.RowMap;
import com.zendesk.maxwell.util.StoppableTask;
import org.apache.pulsar.client.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.*;

public class MaxwellPulsarProducer extends AbstractProducer implements StoppableTask {
	private static final Logger logger = LoggerFactory.getLogger(MaxwellPulsarProducer.class);

	private PulsarClient client;
	private Producer<String> producer;

	private CompletableFuture<Void> close;

	public MaxwellPulsarProducer(MaxwellContext maxwellContext) {
		super(maxwellContext);

		close = new CompletableFuture<>();

		MaxwellConfig cfg = maxwellContext.getConfig();

		try {
			ClientBuilder cb = PulsarClient.builder();
			cb = cb.serviceUrl(cfg.pulsarWebServiceUrl);
			cb = cb.enableTls(cfg.pulsarUseTls);
			cb = cb.allowTlsInsecureConnection(cfg.pulsarTlsAllowInsecureConnection);

			if (cfg.pulsarTlsTrustCertsFilePath != null) {
				cb = cb.tlsTrustCertsFilePath(cfg.pulsarTlsTrustCertsFilePath);
			}

			if (cfg.pulsarAuthPlugin != null) {
				cb = cb.authentication(cfg.pulsarAuthPlugin, cfg.pulsarAuthParams);
			}

			this.client = cb.build();

			ProducerBuilder<String> pb = this.client.newProducer(Schema.STRING);

			pb = pb.topic(cfg.pulsarTopic);
			if (cfg.pulsarProducerName != null) {
				pb = pb.producerName(cfg.pulsarProducerName);
			}

			if (Objects.equals(cfg.pulsarMessageRoutingMode, "single")) {
				pb = pb.messageRoutingMode(MessageRoutingMode.SinglePartition);
			} else {
				if (!Objects.equals(cfg.pulsarMessageRoutingMode, "roundRobin")) {
					logger.info(
							"Initialized with invalid configuration in pulsar_message_routing_mode" +
									" - was expecting either single or roundRobin, got [" +
									cfg.pulsarMessageRoutingMode + "]. Proceeding using roundRobin");
				}

				pb = pb.messageRoutingMode(MessageRoutingMode.RoundRobinPartition);
			}

			if (cfg.pulsarProducerSequenceId > -1) {
				pb = pb.initialSequenceId(cfg.pulsarProducerSequenceId);
			}

			this.producer = pb.create();

		} catch (PulsarClientException ex) {
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void push(RowMap r) throws Exception  {
		if ( !r.shouldOutput(outputConfig) ) {
			context.setPosition(r.getNextPosition());
			return;
		}

		String msg = r.toJSON(outputConfig);
		MessageId msgId = null;

		try {
			msgId = this.producer.send(msg);

			this.succeededMessageCount.inc();
			this.succeededMessageMeter.mark();
		} catch (Exception e) {
			this.failedMessageCount.inc();
			this.failedMessageMeter.mark();
			logger.error("Exception during put", e);

			if (!context.getConfig().ignoreProducerError) {
				throw new RuntimeException(e);
			}
		}

		if ( r.isTXCommit() ) {
			context.setPosition(r.getNextPosition());
		}

		if (logger.isDebugEnabled() && msgId != null) {
			logger.debug("-> #" + msgId + " msg:" + msg);
		}
	}

	@Override
	public StoppableTask getStoppableTask() {
		return this;
	}

	@Override
	public void requestStop() {
		this.close = CompletableFuture.allOf(
				this.client.closeAsync(),
				this.producer.closeAsync()
		);
	}

	@Override
	public void awaitStop(Long timeout) throws TimeoutException {
		try {
			this.close.get(timeout, TimeUnit.MILLISECONDS);
		} catch (InterruptedException | ExecutionException e) {
			// didn't expect to see either of you here.
			throw new RuntimeException(e);
		} catch (TimeoutException e) {
			// transform java native timeout exception to domain-specific TimeoutException
			throw new TimeoutException();
		}
	}
}
