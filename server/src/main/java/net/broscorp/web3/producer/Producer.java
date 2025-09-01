package net.broscorp.web3.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.dto.request.BlocksRequest;
import net.broscorp.web3.dto.request.ClientRequest;
import net.broscorp.web3.dto.request.LogsRequest;
import net.broscorp.web3.service.BlocksService;
import net.broscorp.web3.service.LogsService;
import net.broscorp.web3.subscription.SubscriptionFactory;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.NoOpFlightProducer;
import org.apache.arrow.flight.Ticket;

import java.util.List;

/**
 * Flight producer that serves Ethereum transactions. The Controller, kind of.
 */
@Slf4j
public class Producer extends NoOpFlightProducer {
    private final LogsService logsService;
    private final BlocksService blocksService;
    private final SubscriptionFactory subscriptionFactory;
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public Producer(LogsService logsService, BlocksService blocksService, SubscriptionFactory subscriptionFactory) {
        this.logsService = logsService;
        this.blocksService = blocksService;
        this.subscriptionFactory = subscriptionFactory;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        try {
            ClientRequest request = MAPPER.readValue(ticket.getBytes(), ClientRequest.class);

            switch (request) {
                case LogsRequest logRequest ->
                        logsService.registerNewSubscription(subscriptionFactory.create(listener, logRequest));
                case BlocksRequest blockRequest ->
                        blocksService.registerNewSubscription(subscriptionFactory.create(listener, blockRequest));
            }
        } catch (Exception e) {
            log.error("Failed to route incoming stream request", e);
            listener.error(e);
        }
    }

    @Override
    public FlightInfo getFlightInfo(CallContext context, FlightDescriptor descriptor) {
        return new FlightInfo(null, descriptor, List.of(new FlightEndpoint(new Ticket(descriptor.getCommand()))), -1, -1);
    }
}
