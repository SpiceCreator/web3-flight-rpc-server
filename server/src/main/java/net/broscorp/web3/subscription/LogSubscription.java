package net.broscorp.web3.subscription;

import lombok.Builder;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.dto.request.LogsRequest;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.web3j.protocol.core.methods.response.Log;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * A component that manages a flight RPC subscription to {@link Log} type of data,
 * which is requested by some {@link LogsRequest}.
 * <p>
 *     This component is supposed to encapsulate and hide the details of converting and sending
 *     data in arrow format to one particular client.
 */
public class LogSubscription extends Subscription<Log, LogsRequest> {
    private final Converter converter;

    @Builder
    LogSubscription(FlightProducer.ServerStreamListener listener,
                    VectorSchemaRoot root,
                    LogsRequest clientRequest,
                    BufferAllocator allocator,
                    Converter converter,
                    ExecutorService executor) {
        super(listener, root, allocator, clientRequest, executor);
        this.converter = converter;
    }

    @Override
    protected List<Log> filter(List<Log> originalData) {
        return originalData.parallelStream()
                .filter(logEntry -> {
                    boolean result = true;
                    if (clientRequest.getTopics() != null && !logEntry.getTopics().isEmpty()) {
                        result = clientRequest.getTopics().contains(logEntry.getTopics().getFirst());
                    }
                    if (clientRequest.getContractAddresses() != null)
                        result &= clientRequest.getContractAddresses().stream()
                                .anyMatch(address -> address.equalsIgnoreCase(logEntry.getAddress()));
                    return result;
                })
                .toList();
    }

    @Override
    protected void populate(List<Log> data, VectorSchemaRoot root) {
        converter.logsToArrow(data, root);
    }
}
