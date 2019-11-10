package file;

import block.BlockId;
import block.BlockManager;
import block.BlockManagerId;
import block.IBlock;
import block.IBlockManager;
import constant.ConfigConstants;
import constant.PathConstants;
import util.ByteUtils;
import util.ErrorCode;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class AlphaFile implements IFile {
    // file meta info
    private static class Meta implements Serializable {
        private final int blockSize = ConfigConstants.BLOCK_SIZE;

        private long size;
        private List<HashMap<BlockManagerId, BlockId>> logicBlockList;

        private long pointer;

        Meta(long size, ArrayList<HashMap<BlockManagerId, BlockId>> logicBlockList) {
            this.size = size;
            this.logicBlockList = logicBlockList;
            this.pointer = 0;
        }

        @Override
        public String toString() {
            StringBuilder strBuilder = new StringBuilder();
            strBuilder.append("size: " + size + "\n");
            strBuilder.append("block size: " + blockSize + "\n");
            for (int i = 0; i < logicBlockList.size(); ++i) {
                strBuilder.append(i + ":");
                HashMap<BlockManagerId, BlockId> logicBlockMap = logicBlockList.get(i);
                for (BlockManagerId blockManagerId : logicBlockMap.keySet()) {
                    BlockId blockId = logicBlockMap.get(blockManagerId);
                    strBuilder.append(" [\"" + blockManagerId.getId() + "\"," + blockId.getId() + "]");
                }
                strBuilder.append("\n");
            }
            return strBuilder.toString();
        }
    }

    private final AlphaFileManagerId fileManagerId;
    private final AlphaFileId fileId;
    private Meta meta;

    // create new file under given file manager
    public AlphaFile(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId)
            throw new ErrorCode(ErrorCode.NULL_FILE_ARGUMENT);

        this.fileManagerId = fileManagerId;
        this.fileId = getNewFileId();
        this.meta = new Meta(0, new ArrayList<>());
        writeMeta(this.meta);
    }

    // get existing file under given file manager
    public AlphaFile(AlphaFileManagerId fileManagerId, AlphaFileId fileId) {
        if (null == fileManagerId || null == fileId)
            throw new ErrorCode(ErrorCode.NULL_FILE_ARGUMENT);

        this.fileManagerId = fileManagerId;
        this.fileId = fileId;
        this.meta = readMeta();
    }

    // get id for new file,  add 1 to id count
    private AlphaFileId getNewFileId() {
        File file = new File(PathConstants.FILE_MANAGER_PATH, PathConstants.FILE_ID_COUNT);
        long newFileIdNum;

        try {
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));

            // read long-type file id
            byte[] bytes = new byte[Long.BYTES];
            if (inputStream.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.INVALID_FILE_ID);
            inputStream.close();

            // increase id count
            newFileIdNum = ByteUtils.bytesToLong(bytes) + 1;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        }

        try {
            // update id count file
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
            byte[] bytes = ByteUtils.longToBytes(newFileIdNum);
            outputStream.write(bytes);
            outputStream.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        }

        return new AlphaFileId(PathConstants.FILE_PREFIX + newFileIdNum);
    }

    // write serialized meta info into meta file
    private void writeMeta(Meta meta) {
        AlphaFileManager fileManager = new AlphaFileManager(fileManagerId);
        File file = new File(fileManager.getPath(), fileId.getId() + PathConstants.META_SUFFIX);

        try {
            // write meta object into meta file
            ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file));
            outputStream.writeObject(meta);
            outputStream.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        }
    }

    // write data into blocks with duplication
    private void writeData(byte[] data) {
        long newPos = meta.pointer + data.length;
        if (newPos > meta.size) {
            setSize(newPos);
        }

        long writeLength = data.length;

        List<HashMap<BlockManagerId, BlockId>> logicBlockList = meta.logicBlockList;

        int blockStartNum = (int) meta.pointer / meta.blockSize;
        int startOffset = (int) meta.pointer % meta.blockSize;

        for (int i = blockStartNum; i < logicBlockList.size(); ++i) {
            HashMap<BlockManagerId, BlockId> logicBlockMap = logicBlockList.get(i);

            for (BlockManagerId blockManagerId : logicBlockMap.keySet()) {
                int iter = 0;
                IBlockManager blockManager = new BlockManager(blockManagerId);
                byte[] bytes = blockManager.getBlock(logicBlockMap.get(blockManagerId)).read();
                int startIndex = (i == blockStartNum) ? startOffset : 0;
                for (int j = startIndex; j < bytes.length && iter < writeLength; ++j) {
                    bytes[j] = data[iter++] ;
                }
                IBlock block = blockManager.newBlock(bytes);
                logicBlockMap.replace(blockManagerId, (BlockId)block.getIndexId());
            }
        }

        // update pointer
        meta.pointer += writeLength;
    }

    // get meta object from meta file
    private Meta readMeta() {
        AlphaFileManager fileManager = new AlphaFileManager(fileManagerId);
        File file = new File(fileManager.getPath(), fileId.getId() + PathConstants.META_SUFFIX);

        try {
            ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(file));
            Meta meta = (Meta)inputStream.readObject();
            inputStream.close();
            return meta;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        } catch (ClassNotFoundException e) {
            throw new ErrorCode(ErrorCode.FILE_META_FILE_INVALID);
        }
    }

    // get data of given length from proper blocks
    private byte[] readData(int length) {
        int readLength = (int) Math.min(length, meta.size - meta.pointer);

        List<HashMap<BlockManagerId, BlockId>> logicBlockList = meta.logicBlockList;

        byte[] data = new byte[readLength];
        int blockStartNum = (int) meta.pointer / meta.blockSize;
        int startOffset = (int) meta.pointer % meta.blockSize;
        int iter = 0;

        for (int i = blockStartNum; i < logicBlockList.size(); ++i) {
            HashMap<BlockManagerId, BlockId> logicBlockMap = logicBlockList.get(i);

            boolean isAvailable = true;
            for (BlockManagerId blockManagerId : logicBlockMap.keySet()) {
                int tempIter = iter;

                try {
                    IBlockManager blockManager = new BlockManager(blockManagerId);
                    IBlock block = blockManager.getBlock(logicBlockMap.get(blockManagerId));
                    byte[] bytes = block.read();
                    int startIndex = (i == blockStartNum) ? startOffset : 0;
                    for (int j = startIndex; j < bytes.length && tempIter < readLength; ++j) {
                        data[tempIter++] = bytes[j];
                    }
                    isAvailable = true;
                    iter = tempIter;
                    break;
                } catch (Exception e) {
                    isAvailable = false;
                }
            }
            if (!isAvailable)
                throw new ErrorCode(ErrorCode.UNAVAILABLE_LOGIC_BLOCK);
        }
        meta.pointer += readLength;
        return data;
    }

    // get proper pointer position
    private long getWhere(int where) {
        switch (where) {
            case MOVE_CURR:
                return meta.pointer;
            case MOVE_HEAD:
                return 0;
            case MOVE_TAIL:
                return meta.size;
            default:
                return -1;
        }
    }

    @Override
    public AlphaFileId getFileId() {
        return fileId;
    }

    @Override
    public IFileManager getFileManager() {
        return new AlphaFileManager(fileManagerId);
    }

    @Override
    public byte[] read(int length) {
        if (length < 0)
            throw new ErrorCode(ErrorCode.INVALID_READ_LENGTH);
        move(0, MOVE_HEAD);
        return readData(length);
    }

    @Override
    public void write(byte[] bytes) {
        if (null == bytes)
            throw new ErrorCode(ErrorCode.NULL_FILE_WRITE_IN_DATA);
        writeData(bytes);
        writeMeta(meta);
    }

    @Override
    public long move(long offset, int where) {
        long base = getWhere(where);
        if (base == -1)
            throw new ErrorCode(ErrorCode.INVALID_WHERE_ARG);

        long pos = base + offset;
        if (pos < 0)
            throw new ErrorCode(ErrorCode.INVALID_POINTER_POS_UNDERFLOW);

        meta.pointer = pos;

        return pos;
    }

    @Override
    public void close() {
        writeMeta(meta);
    }

    @Override
    public long size() {
        return meta.size;
    }

    @Override
    public void setSize(long newSize) {
        if (newSize < 0)
            throw new ErrorCode(ErrorCode.NEGATIVE_FILE_NEW_SIZE);

        meta.size = newSize;
        int ceil = (meta.size == 0) ? 0 : (int) meta.size / meta.blockSize + 1;
        if (ceil < meta.logicBlockList.size()) {
            meta.logicBlockList = meta.logicBlockList.subList(0, ceil);
        } else if (ceil > meta.logicBlockList.size()) {
            int newBlockNum = ceil - meta.logicBlockList.size();
            for (int i = 0; i < newBlockNum; ++i) {
                HashMap<BlockManagerId, BlockId> newBlockMap = new HashMap<>();
                for (int j = 0; j < ConfigConstants.DUPLICATION_NUM; ++j) {
                    BlockManager blockManager;
                    do {
                        blockManager = BlockManager.getRandomBlockManager();
                    } while (newBlockMap.containsKey(blockManager.getManagerId()));
                    IBlock block = blockManager.newEmptyBlock(meta.blockSize);
                    newBlockMap.put(blockManager.getManagerId(), (BlockId)block.getIndexId());
                }
                meta.logicBlockList.add(newBlockMap);
            }
        }
    }

    @Override
    public void copyTo(IFile dst) {
        if (null == dst)
            throw new ErrorCode(ErrorCode.NULL_COPY_DST_FILE);

        if (dst.getClass() != AlphaFile.class)
            throw new ErrorCode(ErrorCode.INVALID_COPY_DST_FILE);

        AlphaFile copyDst = (AlphaFile) dst;

        // copy meta info immediately
        copyDst.meta = meta;
        copyDst.close();
    }
}
