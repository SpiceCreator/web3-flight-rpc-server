package net.broscorp.web3.subscription;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.broscorp.web3.dto.request.BlocksRequest;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SubscriptionTest {

    private static final int TEST_TIMEOUT_MILLIS = 5_000;
    @Mock
    private FlightProducer.ServerStreamListener listenerMock;
    @Mock
    private VectorSchemaRoot rootMock;
    private BlocksRequest request;

    private BufferAllocator allocator;
    private ExecutorService testExecutor;

    @BeforeEach
    void setUp() {
        allocator = new RootAllocator();
        testExecutor = Executors.newVirtualThreadPerTaskExecutor();
        doNothing().when(listenerMock).start(any());
    }

    @AfterEach
    void tearDown() {
        if (testExecutor != null && !testExecutor.isShutdown()) {
            testExecutor.shutdownNow();
        }
        allocator.close();
    }

    /**
     * Test implementation of Subscription for simplicity and assertions
     */
    @Slf4j
    private static class TestSubscription extends Subscription<Integer, BlocksRequest> {
        public final AtomicInteger populateCallCount = new AtomicInteger(0);
        public final List<List<Integer>> populatedData = Collections.synchronizedList(new ArrayList<>());

        protected TestSubscription(FlightProducer.ServerStreamListener listener,
                                   VectorSchemaRoot root,
                                   BufferAllocator allocator,
                                   BlocksRequest clientRequest,
                                   ExecutorService executor) {
            super(listener, root, allocator, clientRequest, executor);
        }

        @Override
        protected List<Integer> filter(List<Integer> originalData) {
            return originalData;
        }

        @Override
        protected void populate(List<Integer> data, VectorSchemaRoot root) {
            populateCallCount.incrementAndGet();
            populatedData.add(data);
        }
    }

    @Test
    void sendHistorical_whenInBackfillingState_enqueuesSendOperation() throws InterruptedException {
        // GIVEN
        request = new BlocksRequest();
        request.setStartBlock(BigInteger.ONE);
        TestSubscription sub = new TestSubscription(listenerMock, rootMock, allocator, request, testExecutor);
        when(rootMock.getRowCount()).thenReturn(1);

        // WHEN
        sub.sendHistorical(List.of(1, 2, 3));

        // THEN
        testExecutor.shutdown();
        assertThat(testExecutor.awaitTermination(TEST_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS)).isTrue();

        assertThat(sub.populateCallCount.get()).isEqualTo(1);
        verify(listenerMock).putNext();
    }

    @Test
    void sendHistorical_whenInStreamingState_throwsException() {
        // GIVEN
        request = new BlocksRequest();
        TestSubscription sub = new TestSubscription(listenerMock, rootMock, allocator, request, testExecutor);

        // WHEN / THEN
        assertThatThrownBy(() -> sub.sendHistorical(List.of(1, 2, 3)))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Subscription cannot, or cannot anymore, send historical data.");
    }

    @Test
    @SneakyThrows
    void sendRealtimeAndSendHistorical_thenCompleteBackfill_sendsDataInCorrectOrder() {
        // GIVEN
        request = new BlocksRequest();
        request.setStartBlock(BigInteger.ONE);
        TestSubscription sub = new TestSubscription(listenerMock, rootMock, allocator, request, testExecutor);
        when(rootMock.getRowCount()).thenReturn(1);

        // WHEN
        sub.sendRealtime(List.of(999));
        sub.sendHistorical(List.of(100, 101));
        sub.completeBackfill();
        sub.sendRealtime(List.of(1000));

        // THEN
        verify(listenerMock, timeout(TEST_TIMEOUT_MILLIS).times(3)).putNext();

        assertThat(sub.populatedData).containsExactly(
                List.of(100, 101),
                List.of(999),
                List.of(1000)
        );
    }

    @Test
    @SneakyThrows
    void close_whenCalledMultipleTimes_completesListenerOnlyOnce() {
        // GIVEN
        request = new BlocksRequest();
        TestSubscription sub = new TestSubscription(listenerMock, rootMock, allocator, request, testExecutor);

        // WHEN
        sub.close();
        sub.close();
        testExecutor.submit(() -> {
            try {
                sub.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        // THEN
        verify(listenerMock, times(1)).completed();
    }
}