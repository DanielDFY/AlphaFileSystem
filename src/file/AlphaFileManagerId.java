package file;

import constant.PathConstants;
import id.Id;
import util.ByteUtils;
import util.ErrorCode;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Serializable;

public class AlphaFileManagerId implements Id, Serializable {
    private final String id;

    public AlphaFileManagerId(String id) {
        long idNum = Long.parseLong(id.replaceAll(PathConstants.FILE_MANAGER_PREFIX, ""));

        File file = new File(PathConstants.FILE_MANAGER_PATH, PathConstants.FILE_MANAGER_ID_COUNT);
        long currentFileManagerId;

        try {
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));

            // read long-type block id
            byte[] bytes = new byte[Long.BYTES];
            if (inputStream.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.INVALID_BLOCK_ID);
            inputStream.close();

            // check id
            currentFileManagerId = ByteUtils.bytesToLong(bytes);
            if (currentFileManagerId < idNum)
                throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_ID, id);

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
        String other =  ((AlphaFileManagerId) otherObject).getId();
        return id.compareTo(other) == 0;
    }
}
