package block;

import id.Id;
import util.ErrorCode;

import java.io.Serializable;

public class BlockManagerClientId implements Id, Serializable {
    private static final long serialVersionUID = 8260889869936465997L;

    private final String hostStr;
    private final String blockManagerIdStr;

    public BlockManagerClientId(String hostStr, String blockManagerIdStr) {
        if (null == hostStr || null == blockManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_CLIENT_ID_ARG);
        this.hostStr = hostStr;
        this.blockManagerIdStr = blockManagerIdStr;
    }

    public String getHostStr() {
        return hostStr;
    }
    public String getBlockManagerIdStr() {
        return blockManagerIdStr;
    }

    @Override
    public int hashCode() {
        return (hostStr + "/" + blockManagerIdStr).hashCode();
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
        BlockManagerClientId other = (BlockManagerClientId) otherObject;
        boolean checkHost = hostStr.compareTo(other.getHostStr()) == 0;
        boolean checkBlockManagerServerId = blockManagerIdStr.compareTo(other.getBlockManagerIdStr()) == 0;
        return checkHost && checkBlockManagerServerId;
    }
}
