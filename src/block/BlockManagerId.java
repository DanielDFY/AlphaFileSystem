package block;

import constant.PathConstants;
import id.Id;
import util.ByteUtils;
import util.ErrorCode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;

public class BlockManagerId implements Id, Serializable {
    private static final long serialVersionUID = 6232379530992407887L;

    private final String id;

    public BlockManagerId(String id) {
        long idNum = Long.parseLong(id.replaceAll(PathConstants.BLOCK_MANAGER_PREFIX, ""));

        File file = new File(PathConstants.BLOCK_MANAGER_PATH, PathConstants.BLOCK_MANAGER_ID_COUNT);
        long currentBlockManagerId;

        try {
            RandomAccessFile input = new RandomAccessFile(file, "r");

            // read long-type block id
            byte[] bytes = new byte[Long.BYTES];
            if (input.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.INVALID_BLOCK_ID);
            input.close();

            // check id
            currentBlockManagerId = ByteUtils.bytesToLong(bytes);
            if (currentBlockManagerId < idNum)
                throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_ID, id);

        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public boolean equals(Object otherObject) {
        if(this == otherObject) {
            return true;
        }
        if(otherObject == null) {
            return false;
        }
        if(getClass() != otherObject.getClass()) {
            return false;
        }
        String other =  ((BlockManagerId) otherObject).getId();
        return id.compareTo(other) == 0;
    }
}
