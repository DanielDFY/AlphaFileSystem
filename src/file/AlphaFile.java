package file;

import block.Block;
import block.BlockId;
import block.BlockManager;
import block.BlockManagerClient;
import block.BlockManagerClientId;
import block.BlockManagerId;
import block.BlockManagerServer;
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

public class AlphaFile implements IFile, Serializable {
    private static final long serialVersionUID = -6937137948304956403L;

    // file meta info
    private static class Meta implements Serializable {
        private static final long serialVersionUID = 2533912448887277622L;

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
    private final FieldId fieldId;
    private final AlphaFileId fileId;
    private Meta meta;

    private boolean isClient;
    private String hostName;

    // create new file under given file manager
    public AlphaFile(AlphaFileManagerId fileManagerId, FieldId fieldId) {
        if (null == fileManagerId)
            throw new ErrorCode(ErrorCode.NULL_FILE_ARGUMENT);

        this.fileManagerId = fileManagerId;
        this.fieldId = fieldId;
        this.fileId = getNewFileId();
        this.meta = new Meta(0, new ArrayList<>());
        writeMeta(this.meta);

        this.isClient = false;
        this.hostName = null;
    }

    // get existing file under given file manager
    public AlphaFile(AlphaFileManagerId fileManagerId, AlphaFileId fileId, FieldId fieldId) {
        if (null == fileManagerId || null == fileId)
            throw new ErrorCode(ErrorCode.NULL_FILE_ARGUMENT);

        this.fileManagerId = fileManagerId;
        this.fieldId = fieldId;
        this.fileId = fileId;
        this.meta = readMeta();

        this.isClient = false;
        this.hostName = null;
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
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        try {
            // update id count file
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file));
            byte[] bytes = ByteUtils.longToBytes(newFileIdNum);
            outputStream.write(bytes);
            outputStream.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        return new AlphaFileId(PathConstants.FILE_PREFIX + newFileIdNum);
    }

    // write serialized meta info into meta file
    private void writeMeta(Meta meta) {
        if (isClient) {
            AlphaFileManagerClientId fileManagerClientId = new AlphaFileManagerClientId(hostName, fileManagerId.getId());
            AlphaFileManagerClient fileManager = new AlphaFileManagerClient(fileManagerClientId);
            fileManager.writeRemoteFileMeta(fieldId, this);
        } else {
            IFileManager fileManager = new AlphaFileManagerServer(fileManagerId);
            File file = new File(fileManager.getPath(), fileId.getId() + PathConstants.META_SUFFIX);

            try {
                // write meta object into meta file
                ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(file));
                outputStream.writeObject(meta);
                outputStream.close();
            } catch (IOException e) {
                throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
            }
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
                IBlockManager blockManager;
                if (isClient) {
                    BlockManagerClientId blockManagerClientId = new BlockManagerClientId(hostName, blockManagerId.getId());
                    blockManager = new BlockManagerClient(blockManagerClientId);
                } else {
                    blockManager = new BlockManagerServer(blockManagerId);
                }
                byte[] bytes = blockManager.getBlock(logicBlockMap.get(blockManagerId)).read();
                int startIndex = (i == blockStartNum) ? startOffset : 0;
                for (int j = startIndex; j < bytes.length && iter < writeLength; ++j) {
                    bytes[j] = data[iter++] ;
                }
                Block block = (Block) blockManager.newBlock(bytes);
                logicBlockMap.put(blockManagerId, block.getIndexId());
            }
        }

        // update pointer
        meta.pointer += writeLength;
    }

    // get meta object from meta file
    private Meta readMeta() {
        IFileManager fileManager;
        if (isClient) {
            AlphaFileManagerClientId fileManagerClientId = new AlphaFileManagerClientId(hostName, fileManagerId.getId());
            fileManager = new AlphaFileManagerClient(fileManagerClientId);
        } else {
            fileManager = new AlphaFileManagerServer(fileManagerId);
        }

        File file = new File(fileManager.getPath(), fileId.getId() + PathConstants.META_SUFFIX);

        try {
            ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(file));
            Meta meta = (Meta)inputStream.readObject();
            inputStream.close();
            return meta;
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
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
                    IBlockManager blockManager;
                    if (isClient) {
                        BlockManagerClientId blockManagerClientId = new BlockManagerClientId(hostName, blockManagerId.getId());
                        blockManager = new BlockManagerClient(blockManagerClientId);
                    } else {
                        blockManager = new BlockManagerServer(blockManagerId);
                    }

                    Block block = (Block) blockManager.getBlock(logicBlockMap.get(blockManagerId));
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
        if (isClient) {
            AlphaFileManagerClientId fileManagerClientId = new AlphaFileManagerClientId(hostName, fileManagerId.getId());
            return new AlphaFileManagerClient(fileManagerClientId);
        } else {
            return new AlphaFileManagerServer(fileManagerId);
        }
    }

    @Override
    public byte[] read(int length) {
        if (length < 0)
            throw new ErrorCode(ErrorCode.INVALID_READ_LENGTH);
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

        if (isClient) {
            AlphaFileManagerClientId fileManagerClientId = new AlphaFileManagerClientId(hostName, fileManagerId.getId());
            AlphaFileManagerClient fileManager = new AlphaFileManagerClient(fileManagerClientId);
            meta = fileManager.setRemoteFileSize(fieldId, newSize).meta;
            return;
        }

        meta.size = newSize;
        int ceil = (meta.size == 0) ? 0 : (int) meta.size / meta.blockSize + 1;
        if (ceil < meta.logicBlockList.size()) {
            // truncate redundant blocks
            meta.logicBlockList = meta.logicBlockList.subList(0, ceil);
        } else if (ceil > meta.logicBlockList.size()) {
            // create placeholders for data writing
            int newBlockNum = ceil - meta.logicBlockList.size();
            for (int i = 0; i < newBlockNum; ++i) {
                HashMap<BlockManagerId, BlockId> newBlockMap = new HashMap<>();
                meta.logicBlockList.add(newBlockMap);
                for (int j = 0; j < ConfigConstants.DUPLICATION_NUM; ++j) {
                    // duplications are allocated to different serving block managers
                    BlockManager blockManager;
                    do {
                        blockManager = BlockManagerServer.getRandomServingBlockManager();
                    } while (newBlockMap.containsKey(blockManager.getManagerId()));
                    Block block = (Block) blockManager.newEmptyBlock(meta.blockSize);
                    newBlockMap.put(blockManager.getManagerId(), block.getIndexId());
                }
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

    public void setRemote(String hostName) {
        this.isClient = true;
        this.hostName = hostName;
    }

    public void setLocal() {
        this.isClient = false;
        this.hostName = null;
    }
}
