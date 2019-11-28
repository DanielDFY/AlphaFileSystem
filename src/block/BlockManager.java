package block;

import id.Id;
import util.ByteUtils;
import util.ErrorCode;
import constant.PathConstants;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class BlockManager implements IBlockManager {
    private final BlockManagerId blockManagerId;

    // block manager meta info
    private static class Meta implements Serializable {
        private static final long serialVersionUID = -8399497222255914720L;

        private final Set<BlockId> blockSet;    // record owned blocks

        Meta(Set<BlockId> blockSet) {
            this.blockSet = blockSet;
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder();
            for (BlockId id : blockSet) {
                builder.append(id.getId());
                builder.append("\n");
            }
            return builder.toString();
        }
    }

    // create new block manager
    public BlockManager() {
        this.blockManagerId = getNewManagerId();

        // initialize block manager directory
        init();

        // initialize new meta file with empty block set
        writeMeta(new Meta(new HashSet<>()));
    }

    // get existing block manager
    public BlockManager(BlockManagerId blockManagerId) {
        if (null == blockManagerId)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_ARGUMENT);

        this.blockManagerId = blockManagerId;
    }

    // get id for new block manager,  add 1 to id count
    private BlockManagerId getNewManagerId() {
        File file = new File(PathConstants.BLOCK_MANAGER_PATH, PathConstants.BLOCK_MANAGER_ID_COUNT);
        long newBlockManagerIdNum;

        try {
            RandomAccessFile input = new RandomAccessFile(file, "r");

            // read long-type block manager id count
            byte[] bytes = new byte[Long.BYTES];
            if (input.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.INVALID_BLOCK_MANAGER_ID);
            input.close();

            // increase id count
            newBlockManagerIdNum = ByteUtils.bytesToLong(bytes) + 1;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        try {
            // update id count file
            RandomAccessFile output = new RandomAccessFile(file, "rwd");
            byte[] bytes = ByteUtils.longToBytes(newBlockManagerIdNum);
            output.write(bytes);
            output.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        return new BlockManagerId(PathConstants.BLOCK_MANAGER_PREFIX + newBlockManagerIdNum);
    }

    // write serialized meta info into meta file
    private void writeMeta(Meta meta) {
        File file = new File(PathConstants.BLOCK_MANAGER_PATH, blockManagerId.getId() + PathConstants.META_SUFFIX);

        try {
            RandomAccessFile output = new RandomAccessFile(file, "rwd");
            byte[] metaStr = ByteUtils.objectToSerialize(meta);
            output.write(metaStr);
            output.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }
    }

    // get meta object from meta file
    private Meta readMeta() {
        File file = new File(PathConstants.BLOCK_MANAGER_PATH, blockManagerId.getId() + PathConstants.META_SUFFIX);

        try {
            RandomAccessFile input = new RandomAccessFile(file, "r");
            long metaLength = input.length();
            byte[] metaStr = new byte[(int) metaLength];
            input.read(metaStr);
            input.close();
            return (Meta) ByteUtils.serializeToObject(metaStr);
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        } catch (ClassNotFoundException e) {
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_META_FILE_INVALID);
        }
    }

    // initialize block manager directory
    private void init() {
        File dir = new File(PathConstants.BLOCK_MANAGER_PATH, blockManagerId.getId());
        if (!dir.mkdir())
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, dir.getPath());
    }

    @Override
    public Block getBlock(Id indexId) {
        if (null == indexId)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_ID_ARG);
        if (indexId.getClass() != BlockId.class)
            throw new ErrorCode(ErrorCode.INVALID_BLOCK_ID_ARG);

        BlockId blockId = (BlockId) indexId;

        Meta meta = readMeta();
        Set<BlockId> blockSet = meta.blockSet;

        if (!blockSet.contains(blockId))
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_INDEX_ID, String.valueOf(blockId.getId()));
        else
            return new Block(this.blockManagerId, blockId);
    }

    @Override
    public Block newBlock(byte[] b) {
        if (null == b)
            throw new ErrorCode(ErrorCode.NULL_NEW_BLOCK_DATA);
        Block block = new Block(this.blockManagerId, b);

        Meta meta = readMeta();
        meta.blockSet.add(block.getIndexId());
        writeMeta(meta);

        return block;
    }

    @Override
    public String getPath() {
        return PathConstants.BLOCK_MANAGER_PATH + "/" + blockManagerId.getId();
    }

    @Override
    public BlockManagerId getManagerId() {
        return blockManagerId;
    }
}
