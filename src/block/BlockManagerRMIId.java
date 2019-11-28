package block;

import constant.ConfigConstants;
import id.Id;
import util.ErrorCode;

import java.io.Serializable;

public class BlockManagerRMIId implements Id, Serializable {
    private static final long serialVersionUID = 8260889869936465997L;

    private final String hostStr;
    private final int port;
    private final String blockManagerIdStr;

    public BlockManagerRMIId(String hostStr, int port, String blockManagerIdStr) {
        if (null == hostStr || null == blockManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_CLIENT_ID_ARG);
        this.hostStr = hostStr;
        this.port = port;
        this.blockManagerIdStr = blockManagerIdStr;
    }

    public String getHostStr() {
        return hostStr;
    }
    public int getPort() {
        return port;
    }
    public String getBlockManagerIdStr() {
        return blockManagerIdStr;
    }

    @Override
    public String toString() {
        return ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + hostStr + ":" + port + "/" + blockManagerIdStr;
    }

    @Override
    public int hashCode() { return toString().hashCode(); }

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
        BlockManagerRMIId other = (BlockManagerRMIId) otherObject;
        boolean checkHost = hostStr.compareTo(other.getHostStr()) == 0;
        boolean checkPort = port == other.getPort();
        boolean checkBlockManagerServerId = blockManagerIdStr.compareTo(other.getBlockManagerIdStr()) == 0;
        return checkHost && checkPort && checkBlockManagerServerId;
    }
}
