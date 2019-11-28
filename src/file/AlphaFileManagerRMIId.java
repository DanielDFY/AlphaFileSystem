package file;

import constant.ConfigConstants;
import id.Id;
import util.ErrorCode;

import java.io.Serializable;

public class AlphaFileManagerRMIId implements Id, Serializable {
    private static final long serialVersionUID = -6552401156582888510L;

    private final String hostStr;
    private final int port;
    private final String fileManagerIdStr;

    public AlphaFileManagerRMIId(String hostStr, int port,String fileManagerIdStr) {
        if (null == hostStr || null == fileManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CLIENT_ID_ARG);
        this.hostStr = hostStr;
        this.port = port;
        this.fileManagerIdStr = fileManagerIdStr;
    }

    public String getHostStr() {
        return hostStr;
    }
    public int getPort() {
        return port;
    }
    public String getFileManagerIdStr() {
        return fileManagerIdStr;
    }

    @Override
    public String toString() {
        return ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + hostStr + ":" + port + "/" + fileManagerIdStr;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
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
        AlphaFileManagerRMIId other = (AlphaFileManagerRMIId) otherObject;
        boolean checkHost = hostStr.compareTo(other.getHostStr()) == 0;
        boolean checkPort = port == other.getPort();
        boolean checkBlockManagerServerId = fileManagerIdStr.compareTo(other.getFileManagerIdStr()) == 0;
        return checkHost && checkPort && checkBlockManagerServerId;
    }
}
