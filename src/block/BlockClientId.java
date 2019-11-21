package block;

import id.Id;
import util.ErrorCode;

import java.io.Serializable;

public class BlockClientId implements Id, Serializable {
    private static final long serialVersionUID = 2703122959160133285L;

    private final long id;

    public BlockClientId(long id) {
        if (id <= 0)
            throw new ErrorCode(ErrorCode.BLOCK_CLIENT_INVALID_BLOCK_ID);

        this.id = id;
    }

    public long getId() {
        return id;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(id);
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
        long other =  ((BlockId) otherObject).getId();
        return id == other;
    }
}
