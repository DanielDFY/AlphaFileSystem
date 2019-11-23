package file;

import id.Id;
import util.ErrorCode;

import java.io.Serializable;

public class AlphaFileManagerClientId implements Id, Serializable {
    private static final long serialVersionUID = -6552401156582888510L;

    private final String hostStr;
    private final String fileManagerIdStr;

    public AlphaFileManagerClientId(String hostStr, String fileManagerIdStr) {
        if (null == hostStr || null == fileManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CLIENT_ID_ARG);
        this.hostStr = hostStr;
        this.fileManagerIdStr = fileManagerIdStr;
    }

    public String getHostStr() {
        return hostStr;
    }
    public String getFileManagerIdStr() {
        return fileManagerIdStr;
    }

    @Override
    public int hashCode() {
        return (hostStr + "/" + fileManagerIdStr).hashCode();
    }

    @Override
    public boolean equals(Object otherObject) {
        if (this == otherObject) {
            return true;
        }
        if (otherObject == null) {
            return false;
        }
        if (getClass() != otherObject.getClass()) {
            return false;
        }
        AlphaFileManagerClientId other = (AlphaFileManagerClientId) otherObject;
        boolean checkHost = hostStr.compareTo(other.getHostStr()) == 0;
        boolean checkBlockManagerServerId = fileManagerIdStr.compareTo(other.getFileManagerIdStr()) == 0;
        return checkHost && checkBlockManagerServerId;
    }
}
