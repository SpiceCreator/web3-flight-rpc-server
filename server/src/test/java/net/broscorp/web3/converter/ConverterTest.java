package net.broscorp.web3.converter;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConverterTest {

    private Converter converter;
    private BufferAllocator allocator;

    @BeforeEach
    void setUp() {
        converter = new Converter();
        allocator = new RootAllocator();
    }

    @AfterEach
    void tearDown() {
        allocator.close();
    }

    @Test
    void logsToArrow_withFullData_populatesAllFieldsCorrectly() {
        // GIVEN
        List<Log> logs = List.of(createTestLog("0x111", true, 1L, List.of("topicA", "topicB")));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(converter.getLogSchema(), allocator)) {
            // WHEN
            converter.logsToArrow(logs, root);

            // THEN
            assertThat(root.getRowCount()).isEqualTo(1);

            assertThat(getString(root, "address", 0)).isEqualTo("0x111");
            assertThat(getString(root, "data", 0)).isEqualTo("0xdata1");
            assertThat(getString(root, "transactionHash", 0)).isEqualTo("0xtxHash1");
            assertThat(getString(root, "blockHash", 0)).isEqualTo("0xbkHash1");

            assertThat(getLong(root, "blockNumber", 0)).isEqualTo(1L);
            assertThat(getInt(root, "transactionIndex", 0)).isEqualTo(2);
            assertThat(getInt(root, "logIndex", 0)).isEqualTo(3);
            assertThat(((BitVector) root.getVector("removed")).get(0)).isEqualTo(1);

            List<?> topics = ((ListVector) root.getVector("topics")).getObject(0);
            assertThat(topics)
                    .hasSize(2)
                    .extracting(Object::toString)
                    .containsExactly("topicA", "topicB");
        }
    }

    @Test
    void logsToArrow_withSomeNullData_handlesNullsGracefully() {
        // GIVEN
        List<Log> logs = List.of(createTestLog(null, false, null, null));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(converter.getLogSchema(), allocator)) {
            // WHEN
            converter.logsToArrow(logs, root);

            // THEN
            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(root.getVector("address").isNull(0)).isTrue();
            assertThat(root.getVector("blockNumber").isNull(0)).isTrue();
            assertThat(root.getVector("topics").isNull(0)).isTrue();

            assertThat(((BitVector) root.getVector("removed")).get(0)).isEqualTo(0);
        }
    }

    @Test
    void logsToArrow_withEmptyLists_populatesAllFieldsCorrectly() {
        // GIVEN
        List<Log> logs = List.of(createTestLog("0x111", true, 1L, List.of()));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(converter.getLogSchema(), allocator)) {
            // WHEN
            converter.logsToArrow(logs, root);

            // THEN
            assertThat(root.getRowCount()).isEqualTo(1);

            assertThat(getString(root, "address", 0)).isEqualTo("0x111");
            assertThat(getString(root, "data", 0)).isEqualTo("0xdata1");
            assertThat(getString(root, "transactionHash", 0)).isEqualTo("0xtxHash1");
            assertThat(getString(root, "blockHash", 0)).isEqualTo("0xbkHash1");

            assertThat(getLong(root, "blockNumber", 0)).isEqualTo(1L);
            assertThat(getInt(root, "transactionIndex", 0)).isEqualTo(2);
            assertThat(getInt(root, "logIndex", 0)).isEqualTo(3);
            assertThat(((BitVector) root.getVector("removed")).get(0)).isEqualTo(1);

            List<?> topics = ((ListVector) root.getVector("topics")).getObject(0);
            assertThat(topics).hasSize(0);
        }
    }

    @Test
    void blocksToArrow_withEmptyLists_populatesAllFieldsCorrectly() {
        // GIVEN
        List<EthBlock.Block> blocks = List.of(createTestBlock("0xAAA", true));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(converter.getBlockSchema(), allocator)) {
            // WHEN
            converter.blocksToArrow(blocks, root);

            assertThat(root.getRowCount()).isEqualTo(1);

            assertThat(getLong(root, "number", 0)).isEqualTo(100L);
            assertThat(getString(root, "hash", 0)).isEqualTo("0xAAA");
            assertThat(getString(root, "parentHash", 0)).isEqualTo("0xparentAAA");
            assertThat(getLong(root, "timestamp", 0)).isEqualTo(1672531200L);
            assertThat(getString(root, "author", 0)).isEqualTo("0xAuthor");
            assertThat(getString(root, "miner", 0)).isEqualTo("0xMiner");
            assertThat(getString(root, "transactionsRoot", 0)).isEqualTo("0xtxRoot");
            assertThat(getString(root, "stateRoot", 0)).isEqualTo("0xstateRoot");
            assertThat(getString(root, "receiptsRoot", 0)).isEqualTo("0xreceiptsRoot");
            assertThat(getString(root, "logsBloom", 0)).isEqualTo("0xlogsBloom");
            assertThat(getLong(root, "gasLimit", 0)).isEqualTo(30_000_000L);
            assertThat(getLong(root, "gasUsed", 0)).isEqualTo(50_000L);
            assertThat(getLong(root, "size", 0)).isEqualTo(1024L);
            assertThat(getString(root, "extraData", 0)).isEqualTo("0xextra");
            assertThat(getString(root, "nonce", 0)).isEqualTo("0xnonce");
            assertThat(getString(root, "difficulty", 0)).isEqualTo("0xdiff");
            assertThat(getString(root, "totalDifficulty", 0)).isEqualTo("0xtotalDiff");
            assertThat(getString(root, "mixHash", 0)).isEqualTo("0xmixHash");
            assertThat(getString(root, "sha3Uncles", 0)).isEqualTo("0xsha3Uncles");

            assertThat(((ListVector) root.getVector("transactions")).getObject(0))
                    .hasSize(2).extracting(Object::toString).containsExactly("tx1", "tx2");
            assertThat(((ListVector) root.getVector("uncles")).getObject(0))
                    .hasSize(1).extracting(Object::toString).containsExactly("uncle1");
            assertThat(((ListVector) root.getVector("sealFields")).getObject(0))
                    .hasSize(1).extracting(Object::toString).containsExactly("seal1");
        }
    }

    @Test
    void blocksToArrow_withSomeNullData_handlesNullsGracefully() {
        // GIVEN
        List<EthBlock.Block> blocks = List.of(createTestBlock("0xBBB", false));

        try (VectorSchemaRoot root = VectorSchemaRoot.create(converter.getBlockSchema(), allocator)) {
            // WHEN
            converter.blocksToArrow(blocks, root);

            // THEN
            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(root.getVector("number").isNull(0)).isTrue();
            assertThat(root.getVector("hash").isNull(0)).isTrue();
            assertThat(root.getVector("miner").isNull(0)).isTrue();
            assertThat(root.getVector("author").isNull(0)).isTrue();
            assertThat(root.getVector("difficulty").isNull(0)).isTrue();
        }
    }

    @Test
    void blocksToArrow_withEmptyLists_handlesEmptyListsCorrectly() {
        // GIVEN
        EthBlock.Block block = createTestBlock("0xCCC", true);
        block.setTransactions(Collections.emptyList());
        block.setUncles(Collections.emptyList());
        block.setSealFields(Collections.emptyList());

        List<EthBlock.Block> blocks = List.of(block);

        try (VectorSchemaRoot root = VectorSchemaRoot.create(converter.getBlockSchema(), allocator)) {
            // WHEN
            converter.blocksToArrow(blocks, root);

            // THEN
            assertThat(root.getRowCount()).isEqualTo(1);
            assertThat(((ListVector) root.getVector("transactions")).getObject(0)).isNotNull().isEmpty();
            assertThat(((ListVector) root.getVector("uncles")).getObject(0)).isNotNull().isEmpty();
            assertThat(((ListVector) root.getVector("sealFields")).getObject(0)).isNotNull().isEmpty();
        }
    }

    private Log createTestLog(String address, boolean removed, Long blockNumber, List<String> topics) {
        Log log = new Log();
        log.setRemoved(removed);
        log.setAddress(address);
        log.setBlockNumber(blockNumber != null ? "0x" + BigInteger.valueOf(blockNumber).toString(16) : null);
        log.setData("0xdata1");
        log.setTransactionHash("0xtxHash1");
        log.setBlockHash("0xbkHash1");
        log.setTransactionIndex("0x" + BigInteger.valueOf(2).toString(16));
        log.setLogIndex("0x" + BigInteger.valueOf(3).toString(16));
        log.setTopics(topics);
        return log;
    }

    private EthBlock.Block createTestBlock(String hash, boolean withData) {
        EthBlock.Block block = new EthBlock.Block();
        if (withData) {
            List<String> txHashes = List.of("tx1", "tx2");
            List<EthBlock.TransactionResult> txResults = txHashes.stream()
                    .map(txHash -> {
                        EthBlock.TransactionResult mockedResult = mock(EthBlock.TransactionResult.class);
                        when(mockedResult.get()).thenReturn(txHash);
                        return mockedResult;
                    })
                    .toList();

            block.setNumber("0x" + BigInteger.valueOf(100).toString(16));
            block.setHash(hash);
            block.setParentHash("0xparent" + hash.substring(2));
            block.setTimestamp("0x" + BigInteger.valueOf(1672531200L).toString(16));
            block.setAuthor("0xAuthor");
            block.setMiner("0xMiner");
            block.setTransactionsRoot("0xtxRoot");
            block.setStateRoot("0xstateRoot");
            block.setReceiptsRoot("0xreceiptsRoot");
            block.setLogsBloom("0xlogsBloom");
            block.setGasLimit("0x" + BigInteger.valueOf(30_000_000).toString(16));
            block.setGasUsed("0x" + BigInteger.valueOf(50_000).toString(16));
            block.setSize("0x" + BigInteger.valueOf(1024).toString(16));
            block.setExtraData("0xextra");
            block.setNonce("0xnonce");
            block.setDifficulty("0xdiff");
            block.setTotalDifficulty("0xtotalDiff");
            block.setMixHash("0xmixHash");
            block.setSha3Uncles("0xsha3Uncles");
            block.setTransactions(txResults);
            block.setUncles(List.of("uncle1"));
            block.setSealFields(List.of("seal1"));
        } else {
            block.setNumber(null);
            block.setHash(null);
            block.setParentHash(null);
            block.setTimestamp(null);
            block.setAuthor(null);
            block.setMiner(null);
            block.setTransactionsRoot(null);
            block.setStateRoot(null);
            block.setReceiptsRoot(null);
            block.setLogsBloom(null);
            block.setGasLimit(null);
            block.setGasUsed(null);
            block.setSize(null);
            block.setExtraData(null);
            block.setNonce(null);
            block.setDifficulty(null);
            block.setTotalDifficulty(null);
            block.setMixHash(null);
            block.setSha3Uncles(null);
            block.setTransactions(List.of());
            block.setUncles(List.of());
            block.setSealFields(List.of());
        }
        return block;
    }

    private String getString(VectorSchemaRoot root, String name, int index) {
        VarCharVector vector = (VarCharVector) root.getVector(name);
        return vector.isNull(index) ? null : new String(vector.get(index), StandardCharsets.UTF_8);
    }

    private Long getLong(VectorSchemaRoot root, String name, int index) {
        BigIntVector vector = (BigIntVector) root.getVector(name);
        return vector.isNull(index) ? null : vector.get(index);
    }

    private Integer getInt(VectorSchemaRoot root, String name, int index) {
        IntVector vector = (IntVector) root.getVector(name);
        return vector.isNull(index) ? null : vector.get(index);
    }
}