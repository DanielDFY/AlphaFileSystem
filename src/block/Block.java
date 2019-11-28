package block;

import constant.PathConstants;
import constant.ConfigConstants;
import util.ByteUtils;
import util.ErrorCode;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Block implements IBlock, Serializable {
    private static final long serialVersionUID = -669111775113682194L;

    // set block size by config
    private final static int SIZE = ConfigConstants.BLOCK_SIZE;

    // block meta info
    private static class Meta implements Serializable {
        private static final long serialVersionUID = 4023762168474307036L;

        private final int size;
        private final String checksum;

        Meta(int size, String checksum) {
            this.size = size;
            this.checksum = checksum;
        }

        @Override
        public String toString() {
            return "size: " + size + "\n checksum: " + checksum;
        }
    }

    private final BlockManagerId blockManagerId;
    private final BlockId blockId;
    private final Meta meta;
    private final byte[] data;

    // get existing block under given block manager
    public Block(BlockManagerId blockManagerId, BlockId blockId) {
        if (null == blockManagerId || null == blockId)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_ARGUMENT);

        this.blockManagerId = blockManagerId;
        this.blockId = blockId;
        this.meta = readMeta();
        this.data = readData();
    }

    // create new block with data under given block manager
    public Block(BlockManagerId blockManagerId, byte[] data) {
        if (null == blockManagerId || null == data)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_ARGUMENT);

        byte[] bytes = align(data);

        this.blockManagerId = blockManagerId;
        this.blockId = getNewBlockId();

        this.meta = writeMeta(bytes);
        this.data = writeData(bytes);
    }

    // put data into block size array
    private byte[] align(byte[] data) {
        if (data.length > SIZE)
            throw new ErrorCode(ErrorCode.INVALID_BLOCK_DATA);

        byte[] bytes;
        if (data.length < SIZE) {
            bytes = new byte[SIZE];
            System.arraycopy(data, 0, bytes, 0, data.length);
        } else {
            bytes = data;
        }

        return bytes;
    }

    // get id for new block,  add 1 to id count
    private BlockId getNewBlockId() {
        File file = new File(PathConstants.BLOCK_MANAGER_PATH, PathConstants.BLOCK_ID_COUNT);
        long newBlockId;

        try {
            RandomAccessFile input = new RandomAccessFile(file, "r");

            // read long-type block id
            byte[] bytes = new byte[Long.BYTES];
            if (input.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.INVALID_BLOCK_ID);
            input.close();

            // increase id count
            newBlockId = ByteUtils.bytesToLong(bytes) + 1;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        try {
            // update id count file
            RandomAccessFile output = new RandomAccessFile(file, "rwd");
            byte[] bytes = ByteUtils.longToBytes(newBlockId);
            output.write(bytes);
            output.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        return new BlockId(newBlockId);
    }

    // write serialized meta info into meta file
    private Meta writeMeta(byte[] data) {
        BlockManagerServer blockManagerServer = BlockManagerServer.getServer(blockManagerId);
        File file = new File(blockManagerServer.getPath(), blockId.getId() + PathConstants.META_SUFFIX);

        try {
            RandomAccessFile output = new RandomAccessFile(file, "rwd");

            // use MD5 to calculate checksum
            MessageDigest MD5 = MessageDigest.getInstance("MD5");
            MD5.update(data);
            String checksum = Arrays.toString(MD5.digest());

            // write meta object into meta file
            Meta meta = new Meta(SIZE, checksum);

            String metaStr = SIZE + "\n" + checksum;
            output.write(metaStr.getBytes());
            output.close();

            return meta;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        } catch (NoSuchAlgorithmException e) {
            throw new ErrorCode(ErrorCode.MD5_INVALID);
        }
    }

    // write data into data file
    private byte[] writeData(byte[] data) {
        BlockManagerServer blockManagerServer = BlockManagerServer.getServer(blockManagerId);
        File file = new File(blockManagerServer.getPath(), blockId.getId() + PathConstants.DATA_SUFFIX);

        try {
            RandomAccessFile output = new RandomAccessFile(file, "rwd");
            output.write(data);
            output.close();

            return data;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }
    }

    // get meta object from meta file
    private Meta readMeta() {
        BlockManagerServer blockManagerServer = BlockManagerServer.getServer(blockManagerId);
        File file = new File(blockManagerServer.getPath(), blockId.getId() + PathConstants.META_SUFFIX);

        try {
            RandomAccessFile input = new RandomAccessFile(file, "r");

            int metaSize = Integer.parseInt(input.readLine());
            String metaChecksum = input.readLine();

            Meta meta = new Meta(metaSize, metaChecksum);
            input.close();
            return meta;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        } catch (Exception e) {
            throw new ErrorCode(ErrorCode.BLOCK_META_FILE_INVALID);
        }
    }

    // get data from data file
    private byte[] readData() {
        BlockManagerServer blockManagerServer = BlockManagerServer.getServer(blockManagerId);
        File file = new File(blockManagerServer.getPath(), blockId.getId() + PathConstants.DATA_SUFFIX);

        try {
            RandomAccessFile input = new RandomAccessFile(file, "r");
            byte[] bytes = new byte[SIZE];
            if (input.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.BLOCK_DATA_FILE_INVALID);
            input.close();
            return bytes;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }
    }

    @Override
    public BlockId getIndexId() {
        return blockId;
    }

    @Override
    public BlockManagerServer getBlockManager() {
        return BlockManagerServer.getServer(blockManagerId);
    }

    @Override
    public byte[] read() {
        MessageDigest MD5;

        try {
            MD5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new ErrorCode(ErrorCode.MD5_INVALID);
        }

        MD5.update(data);
        String checksum = Arrays.toString(MD5.digest());

        // verify checksum
        if (checksum.compareTo(meta.checksum) != 0) {
            throw new ErrorCode(ErrorCode.CHECKSUM_CHECK_FAILED);
        }

        return data;
    }

    @Override
    public int blockSize() {
        Meta meta = readMeta();
        return meta.size;
    }
}
