package net.broscorp.web3.subscription;

import lombok.Builder;
import net.broscorp.web3.converter.Converter;
import net.broscorp.web3.dto.request.BlocksRequest;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.web3j.protocol.core.methods.response.EthBlock;

import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * A component that manages a flight RPC subscription to {@link EthBlock.Block} type of data,
 * which is requested by some {@link BlocksRequest}.
 * <p>
 *     This component is supposed to encapsulate and hide the details of converting and sending
 *     data in arrow format to one particular client.
 */
public class BlockSubscription extends Subscription<EthBlock.Block, BlocksRequest> {
    private final Converter converter;

    @Builder
    BlockSubscription(FlightProducer.ServerStreamListener listener,
                      VectorSchemaRoot root,
                      BlocksRequest clientRequest,
                      BufferAllocator allocator,
                      Converter converter,
                      ExecutorService executor) {
        super(listener, root, allocator, clientRequest, executor);
        this.converter = converter;
    }

    @Override
    protected List<EthBlock.Block> filter(List<EthBlock.Block> originalData) {
        return originalData;
    }

    @Override
    protected void populate(List<EthBlock.Block> data, VectorSchemaRoot root) {
        converter.blocksToArrow(data, root);
    }
}
