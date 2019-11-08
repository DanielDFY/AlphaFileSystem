package block;

import constant.PathConstants;
import constant.ConfigConstants;
import util.ByteUtils;
import util.ErrorCode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class Block implements IBlock {
    // set block size by config
    private final static int SIZE = ConfigConstants.BLOCK_SIZE;

    // block meta info
    private static class Meta implements Serializable {
        private final int size;
        private final String checksum;

        Meta(int size, String checksum) {
            this.size = size;
            this.checksum = checksum;
        }

        @Override
        public String toString() {
            return "size: " + size + ", checksum: " + checksum;
        }
    }

    private final BlockManagerId blockManagerId;
    private final BlockId blockId;

    // get existing block under given block manager
    public Block(BlockManagerId blockManagerId, BlockId blockId) {
        if (null == blockManagerId || null == blockId)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_ARGUMENT);

        this.blockManagerId = blockManagerId;
        this.blockId = blockId;
    }

    // create new block with data under given block manager
    public Block(BlockManagerId blockManagerId, byte[] data) {
        if (null == blockManagerId || null == data)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_ARGUMENT);

        byte[] bytes = align(data);

        this.blockManagerId = blockManagerId;
        this.blockId = getNewBlockId();

        writeMeta(bytes);
        writeData(bytes);
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
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));

            // read long-type block id
            byte[] bytes = new byte[Long.BYTES];
            if (inputStream.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.INVALID_BLOCK_ID);
            inputStream.close();

            // increase id count
            newBlockId = ByteUtils.bytesToLong(bytes) + 1;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        }

        try {
            // update id count file
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
            byte[] bytes = ByteUtils.longToBytes(newBlockId);
            outputStream.write(bytes);
            outputStream.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        }

        return new BlockId(newBlockId);
    }

    // write serialized meta info into meta file
    private void writeMeta(byte[] data) {
        BlockManager blockManager = new BlockManager(blockManagerId);
        File file = new File(blockManager.getPath(), blockId.getId() + PathConstants.META_SUFFIX);

        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file));

            // use MD5 to calculate checksum
            MessageDigest MD5 = MessageDigest.getInstance("MD5");
            MD5.update(data);
            String checksum = Arrays.toString(MD5.digest());

            // write meta object into meta file
            Meta meta = new Meta(SIZE, checksum);
            outputStream.writeObject(meta);
            outputStream.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        } catch (NoSuchAlgorithmException e) {
            throw new ErrorCode(ErrorCode.MD5_INVALID);
        }
    }

    // write data into data file
    private void writeData(byte[] data) {
        BlockManager blockManager = new BlockManager(blockManagerId);
        File file = new File(blockManager.getPath(), blockId.getId() + PathConstants.DATA_SUFFIX);

        try {
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
            outputStream.write(data);
            outputStream.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        }
    }

    // get meta object from meta file
    private Meta readMeta() {
        BlockManager blockManager = new BlockManager(blockManagerId);
        File file = new File(blockManager.getPath(), blockId.getId() + PathConstants.META_SUFFIX);

        try {
            ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(file));
            Meta meta = (Meta)inputStream.readObject();
            inputStream.close();
            return meta;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        } catch (ClassNotFoundException e) {
            throw new ErrorCode(ErrorCode.BLOCK_META_FILE_INVALID);
        }
    }

    // get data from data file
    private byte[] readData() {
        BlockManager blockManager = new BlockManager(blockManagerId);
        File file = new File(blockManager.getPath(), blockId.getId() + PathConstants.DATA_SUFFIX);

        try {
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));
            byte[] bytes = new byte[SIZE];
            if (inputStream.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.BLOCK_DATA_FILE_INVALID);
            inputStream.close();
            return bytes;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        }
    }

    @Override
    public BlockId getIndexId() {
        return blockId;
    }

    @Override
    public IBlockManager getBlockManager() {
        return new BlockManager(blockManagerId);
    }

    @Override
    public byte[] read() {
        Meta meta = readMeta();
        byte[] data = readData();

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
