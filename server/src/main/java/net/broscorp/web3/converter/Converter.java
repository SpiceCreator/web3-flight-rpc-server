package net.broscorp.web3.converter;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.Log;
import org.web3j.utils.Numeric;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A utility class for converting Ethereum logs and blocks to the Arrow format.
 * This class defines a schema for logs and blocks and provides methods to perform the conversion.
 */
@Slf4j
public final class Converter {

    private static final String LOG_ADDRESS = "address";
    private static final String LOG_DATA = "data";
    private static final String LOG_TOPICS = "topics";
    private static final String LOG_TOPIC_CHILD_NAME = "topic";
    private static final String LOG_BLOCK_NUMBER = "blockNumber";
    private static final String LOG_TRANSACTION_HASH = "transactionHash";
    private static final String LOG_TRANSACTION_INDEX = "transactionIndex";
    private static final String LOG_BLOCK_HASH = "blockHash";
    private static final String LOG_INDEX = "logIndex";
    private static final String LOG_REMOVED = "removed";

    private static final Schema LOG_SCHEMA = new Schema(List.of(
            Field.nullable(LOG_ADDRESS, new ArrowType.Utf8()),
            Field.nullable(LOG_DATA, new ArrowType.Utf8()),
            new Field(LOG_TOPICS, new FieldType(true, new ArrowType.List(), null),
                    Lists.newArrayList(Field.nullable(LOG_TOPIC_CHILD_NAME, new ArrowType.Utf8()))),
            Field.nullable(LOG_BLOCK_NUMBER, new ArrowType.Int(64, true)),
            Field.nullable(LOG_TRANSACTION_HASH, new ArrowType.Utf8()),
            Field.nullable(LOG_TRANSACTION_INDEX, new ArrowType.Int(32, true)),
            Field.nullable(LOG_BLOCK_HASH, new ArrowType.Utf8()),
            Field.nullable(LOG_INDEX, new ArrowType.Int(32, true)),
            Field.nullable(LOG_REMOVED, new ArrowType.Bool())
    ));

    private static final String BLOCK_NUMBER = "number";
    private static final String BLOCK_HASH_FIELD = "hash";
    private static final String BLOCK_PARENT_HASH = "parentHash";
    private static final String BLOCK_NONCE = "nonce";
    private static final String BLOCK_SHA3_UNCLES = "sha3Uncles";
    private static final String BLOCK_LOGS_BLOOM = "logsBloom";
    private static final String BLOCK_TRANSACTIONS_ROOT = "transactionsRoot";
    private static final String BLOCK_STATE_ROOT = "stateRoot";
    private static final String BLOCK_RECEIPTS_ROOT = "receiptsRoot";
    private static final String BLOCK_AUTHOR = "author";
    private static final String BLOCK_MINER = "miner";
    private static final String BLOCK_MIX_HASH = "mixHash";
    private static final String BLOCK_DIFFICULTY = "difficulty";
    private static final String BLOCK_TOTAL_DIFFICULTY = "totalDifficulty";
    private static final String BLOCK_EXTRA_DATA = "extraData";
    private static final String BLOCK_SIZE = "size";
    private static final String BLOCK_GAS_LIMIT = "gasLimit";
    private static final String BLOCK_GAS_USED = "gasUsed";
    private static final String BLOCK_TIMESTAMP = "timestamp";
    private static final String BLOCK_TRANSACTIONS = "transactions";
    private static final String BLOCK_UNCLES = "uncles";
    private static final String BLOCK_SEAL_FIELDS = "sealFields";
    private static final String BLOCK_TX_CHILD_NAME = "txHash";
    private static final String BLOCK_UNCLE_CHILD_NAME = "uncleHash";
    private static final String BLOCK_SEAL_FIELD_CHILD_NAME = "sealField";


    private static final Schema BLOCK_SCHEMA = new Schema(List.of(
            Field.nullable(BLOCK_NUMBER, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_HASH_FIELD, new ArrowType.Utf8()),
            Field.nullable(BLOCK_PARENT_HASH, new ArrowType.Utf8()),
            Field.nullable(BLOCK_TIMESTAMP, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_AUTHOR, new ArrowType.Utf8()),
            Field.nullable(BLOCK_MINER, new ArrowType.Utf8()),
            Field.nullable(BLOCK_TRANSACTIONS_ROOT, new ArrowType.Utf8()),
            Field.nullable(BLOCK_STATE_ROOT, new ArrowType.Utf8()),
            Field.nullable(BLOCK_RECEIPTS_ROOT, new ArrowType.Utf8()),
            Field.nullable(BLOCK_LOGS_BLOOM, new ArrowType.Utf8()),
            Field.nullable(BLOCK_GAS_LIMIT, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_GAS_USED, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_SIZE, new ArrowType.Int(64, true)),
            Field.nullable(BLOCK_EXTRA_DATA, new ArrowType.Utf8()),
            Field.nullable(BLOCK_NONCE, new ArrowType.Utf8()),
            Field.nullable(BLOCK_DIFFICULTY, new ArrowType.Utf8()),
            Field.nullable(BLOCK_TOTAL_DIFFICULTY, new ArrowType.Utf8()),
            Field.nullable(BLOCK_MIX_HASH, new ArrowType.Utf8()),
            Field.nullable(BLOCK_SHA3_UNCLES, new ArrowType.Utf8()),
            new Field(BLOCK_TRANSACTIONS, new FieldType(true, new ArrowType.List(), null),
                    Lists.newArrayList(Field.nullable(BLOCK_TX_CHILD_NAME, new ArrowType.Utf8()))),
            new Field(BLOCK_UNCLES, new FieldType(true, new ArrowType.List(), null),
                    Lists.newArrayList(Field.nullable(BLOCK_UNCLE_CHILD_NAME, new ArrowType.Utf8()))),
            new Field(BLOCK_SEAL_FIELDS, new FieldType(true, new ArrowType.List(), null),
                    Lists.newArrayList(Field.nullable(BLOCK_SEAL_FIELD_CHILD_NAME, new ArrowType.Utf8())))
    ));

    /**
     * Returns the instance of the log Schema.
     */
    public Schema getLogSchema() {
        return LOG_SCHEMA;
    }

    /**
     * Returns the instance of the block Schema.
     */
    public Schema getBlockSchema() {
        return BLOCK_SCHEMA;
    }

    /**
     * Populates a VectorSchemaRoot with data from a list of web3j Log objects.
     */
    public void logsToArrow(List<Log> logs, VectorSchemaRoot root) {
        final int batchSize = logs.size();

        final VarCharVector addressVector = (VarCharVector) root.getVector(LOG_ADDRESS);
        final VarCharVector dataVector = (VarCharVector) root.getVector(LOG_DATA);
        final ListVector topicsVector = (ListVector) root.getVector(LOG_TOPICS);
        final BigIntVector blockNumberVector = (BigIntVector) root.getVector(LOG_BLOCK_NUMBER);
        final VarCharVector transactionHashVector = (VarCharVector) root.getVector(LOG_TRANSACTION_HASH);
        final IntVector transactionIndexVector = (IntVector) root.getVector(LOG_TRANSACTION_INDEX);
        final VarCharVector blockHashVector = (VarCharVector) root.getVector(LOG_BLOCK_HASH);
        final IntVector logIndexVector = (IntVector) root.getVector(LOG_INDEX);
        final BitVector removedVector = (BitVector) root.getVector(LOG_REMOVED);

        blockNumberVector.allocateNew(batchSize);
        transactionIndexVector.allocateNew(batchSize);
        logIndexVector.allocateNew(batchSize);
        removedVector.allocateNew(batchSize);
        topicsVector.allocateNew();

        long totalAddressBytes = 0, totalDataBytes = 0, totalTxHashBytes = 0, totalBlockHashBytes = 0;
        for (Log log : logs) {
            totalAddressBytes += getStrLen(log.getAddress());
            totalDataBytes += getStrLen(log.getData());
            totalTxHashBytes += getStrLen(log.getTransactionHash());
            totalBlockHashBytes += getStrLen(log.getBlockHash());
        }

        addressVector.allocateNew(totalAddressBytes, batchSize);
        dataVector.allocateNew(totalDataBytes, batchSize);
        transactionHashVector.allocateNew(totalTxHashBytes, batchSize);
        blockHashVector.allocateNew(totalBlockHashBytes, batchSize);

        final UnionListWriter topicsWriter = topicsVector.getWriter();
        for (int i = 0; i < batchSize; i++) {
            final Log logEntry = logs.get(i);

            setNullableString(addressVector, i, logEntry.getAddress());
            setNullableString(dataVector, i, logEntry.getData());
            setNullableString(transactionHashVector, i, logEntry.getTransactionHash());
            setNullableString(blockHashVector, i, logEntry.getBlockHash());
            setNullableHexAsLong(blockNumberVector, i, logEntry.getBlockNumberRaw());
            setNullableHexAsInt(transactionIndexVector, i, logEntry.getTransactionIndexRaw());
            setNullableHexAsInt(logIndexVector, i, logEntry.getLogIndexRaw());
            setNullableBool(removedVector, i, logEntry.isRemoved());

            final List<String> topics = logEntry.getTopics();
            populateStringList(topicsWriter, i, topics);
        }
        root.setRowCount(batchSize);
    }

    /**
     * Populates a VectorSchemaRoot with data from a list of web3j Block objects.
     * Assumes this object does not contain full transaction objects, instead only their hashes.
     */
    public void blocksToArrow(List<EthBlock.Block> blocks, VectorSchemaRoot root) {
        final int batchSize = blocks.size();

        final BigIntVector numberVector = (BigIntVector) root.getVector(BLOCK_NUMBER);
        final BigIntVector timestampVector = (BigIntVector) root.getVector(BLOCK_TIMESTAMP);
        final BigIntVector sizeVector = (BigIntVector) root.getVector(BLOCK_SIZE);
        final BigIntVector gasLimitVector = (BigIntVector) root.getVector(BLOCK_GAS_LIMIT);
        final BigIntVector gasUsedVector = (BigIntVector) root.getVector(BLOCK_GAS_USED);
        final ListVector transactionsVector = (ListVector) root.getVector(BLOCK_TRANSACTIONS);
        final ListVector unclesVector = (ListVector) root.getVector(BLOCK_UNCLES);
        final ListVector sealFieldsVector = (ListVector) root.getVector(BLOCK_SEAL_FIELDS);

        numberVector.allocateNew(batchSize);
        timestampVector.allocateNew(batchSize);
        sizeVector.allocateNew(batchSize);
        gasLimitVector.allocateNew(batchSize);
        gasUsedVector.allocateNew(batchSize);
        transactionsVector.allocateNew();
        unclesVector.allocateNew();
        sealFieldsVector.allocateNew();

        final VarCharVector hashVector = (VarCharVector) root.getVector(BLOCK_HASH_FIELD);
        final VarCharVector parentHashVector = (VarCharVector) root.getVector(BLOCK_PARENT_HASH);
        final VarCharVector nonceVector = (VarCharVector) root.getVector(BLOCK_NONCE);
        final VarCharVector minerVector = (VarCharVector) root.getVector(BLOCK_MINER);
        final VarCharVector difficultyVector = (VarCharVector) root.getVector(BLOCK_DIFFICULTY);
        final VarCharVector totalDifficultyVector = (VarCharVector) root.getVector(BLOCK_TOTAL_DIFFICULTY);
        final VarCharVector authorVector = (VarCharVector) root.getVector(BLOCK_AUTHOR);
        final VarCharVector blockMixHashVector = (VarCharVector) root.getVector(BLOCK_MIX_HASH);
        final VarCharVector blockSha3UnclesVector = (VarCharVector) root.getVector(BLOCK_SHA3_UNCLES);
        final VarCharVector blockStateRootVector = (VarCharVector) root.getVector(BLOCK_STATE_ROOT);
        final VarCharVector blockReceiptsVector = (VarCharVector) root.getVector(BLOCK_RECEIPTS_ROOT);
        final VarCharVector blockLogsBloomVector = (VarCharVector) root.getVector(BLOCK_LOGS_BLOOM);
        final VarCharVector blockTransactionRootVector = (VarCharVector) root.getVector(BLOCK_TRANSACTIONS_ROOT);
        final VarCharVector blockExtraDataVector = (VarCharVector) root.getVector(BLOCK_EXTRA_DATA);

        long totalHashFieldBytes = 0, totalBlockParentHashBytes = 0, totalNonceBytes = 0, totalMinerBytes = 0,
                totalDifficultyBytes = 0, totalTotalDifficultyBytes = 0, totalAuthorBytes = 0, totalMixHashVectorBytes = 0,
                totalSha3UnclesBytes = 0, totalStateRootBytes = 0, totalReceiptsBytes = 0, totalLogsBloomBytes = 0,
                totalTransactionRootBytes = 0, totalExtraDataBytes = 0;

        for (EthBlock.Block block : blocks) {
            totalHashFieldBytes += getStrLen(block.getHash());
            totalBlockParentHashBytes += getStrLen(block.getParentHash());
            totalNonceBytes += getStrLen(block.getNonceRaw());
            totalMinerBytes += getStrLen(block.getMiner());
            totalDifficultyBytes += getStrLen(block.getDifficultyRaw());
            totalTotalDifficultyBytes += getStrLen(block.getTotalDifficultyRaw());
            totalAuthorBytes += getStrLen(block.getAuthor());
            totalMixHashVectorBytes += getStrLen(block.getMixHash());
            totalSha3UnclesBytes += getStrLen(block.getSha3Uncles());
            totalStateRootBytes += getStrLen(block.getStateRoot());
            totalReceiptsBytes += getStrLen(block.getReceiptsRoot());
            totalLogsBloomBytes += getStrLen(block.getLogsBloom());
            totalTransactionRootBytes += getStrLen(block.getTransactionsRoot());
            totalExtraDataBytes += getStrLen(block.getExtraData());
        }

        hashVector.allocateNew(totalHashFieldBytes, batchSize);
        parentHashVector.allocateNew(totalBlockParentHashBytes, batchSize);
        nonceVector.allocateNew(totalNonceBytes, batchSize);
        difficultyVector.allocateNew(totalDifficultyBytes, batchSize);
        totalDifficultyVector.allocateNew(totalTotalDifficultyBytes, batchSize);
        authorVector.allocateNew(totalAuthorBytes, batchSize);
        blockMixHashVector.allocateNew(totalMixHashVectorBytes, batchSize);
        minerVector.allocateNew(totalMinerBytes, batchSize);
        blockSha3UnclesVector.allocateNew(totalSha3UnclesBytes, batchSize);
        blockStateRootVector.allocateNew(totalStateRootBytes, batchSize);
        blockReceiptsVector.allocateNew(totalReceiptsBytes, batchSize);
        blockLogsBloomVector.allocateNew(totalLogsBloomBytes, batchSize);
        blockTransactionRootVector.allocateNew(totalTransactionRootBytes, batchSize);
        blockExtraDataVector.allocateNew(totalExtraDataBytes, batchSize);

        for (int i = 0; i < batchSize; i++) {
            final EthBlock.Block block = blocks.get(i);

            setNullableHexAsLong(numberVector, i, block.getNumberRaw());
            setNullableHexAsLong(timestampVector, i, block.getTimestampRaw());
            setNullableHexAsLong(gasLimitVector, i, block.getGasLimitRaw());
            setNullableHexAsLong(gasUsedVector, i, block.getGasUsedRaw());
            setNullableHexAsLong(sizeVector, i, block.getSizeRaw());

            setNullableString(hashVector, i, block.getHash());
            setNullableString(parentHashVector, i, block.getParentHash());
            setNullableString(nonceVector, i, block.getNonceRaw());
            setNullableString(minerVector, i, block.getMiner());
            setNullableString(difficultyVector, i, block.getDifficultyRaw());
            setNullableString(totalDifficultyVector, i, block.getTotalDifficultyRaw());
            setNullableString(authorVector, i, block.getAuthor());
            setNullableString(blockMixHashVector, i, block.getMixHash());
            setNullableString(blockSha3UnclesVector, i, block.getSha3Uncles());
            setNullableString(blockStateRootVector, i, block.getStateRoot());
            setNullableString(blockReceiptsVector, i, block.getReceiptsRoot());
            setNullableString(blockLogsBloomVector, i, block.getLogsBloom());
            setNullableString(blockTransactionRootVector, i, block.getTransactionsRoot());
            setNullableString(blockExtraDataVector, i, block.getExtraData());

            final UnionListWriter txWriter = transactionsVector.getWriter();
            final UnionListWriter unclesWriter = unclesVector.getWriter();
            final UnionListWriter sealFieldsWriter = sealFieldsVector.getWriter();
            populateStringList(txWriter, i, block.getTransactions().stream()
                    .map(tx -> {
                        if (tx.get() instanceof EthBlock.TransactionObject)
                            return ((EthBlock.TransactionObject) tx.get()).get().getHash();
                        else
                            return tx.get().toString();
                    }).toList());
            populateStringList(unclesWriter, i, block.getUncles());
            populateStringList(sealFieldsWriter, i, block.getSealFields());
        }

        root.setRowCount(batchSize);
    }

    private static void populateStringList(UnionListWriter writer, int index, List<String> list) {
        writer.setPosition(index);
        if (list != null) {
            writer.startList();
            for (String item : list) {
                writer.writeVarChar(item);
            }
            writer.endList();
        }
    }

    private static void setNullableString(VarCharVector vector, int index, String value) {
        if (value != null) {
            vector.set(index, value.getBytes(StandardCharsets.UTF_8));
        } else {
            vector.setNull(index);
        }
    }

    private static void setNullableBool(BitVector vector, int index, Boolean value) {
        if (value != null) {
            vector.set(index, value ? 1 : 0);
        } else {
            vector.setNull(index);
        }
    }

    private static void setNullableHexAsLong(BigIntVector vector, int index, String hexValue) {
        if (hexValue != null) {
            vector.set(index, Numeric.decodeQuantity(hexValue).longValue());
        } else {
            vector.setNull(index);
        }
    }

    private static void setNullableHexAsInt(IntVector vector, int index, String hexValue) {
        if (hexValue != null) {
            vector.set(index, Numeric.decodeQuantity(hexValue).intValue());
        } else {
            vector.setNull(index);
        }
    }

    private static int getStrLen(String s) {
        return s != null ? s.getBytes(StandardCharsets.UTF_8).length : 0;
    }
}