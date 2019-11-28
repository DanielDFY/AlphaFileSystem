package block;

import util.ErrorCode;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class BlockManagerRMI extends UnicastRemoteObject implements IBlockManagerRMI {
    private BlockManagerRMIId id;

    public BlockManagerRMI(BlockManagerRMIId id) throws RemoteException {
        super();
        if (null == id)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_RMI_ID_ARG);
        this.id = id;
    }

    @Override
    public IBlock getBlockRMI(long blockIdNum) {
        if (0 == blockIdNum)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_RMI_ID_ARG);

        BlockManagerId blockManagerId = new BlockManagerId(id.getBlockManagerIdStr());
        IBlockManager blockManagerServer = BlockManagerServer.getServer(blockManagerId);

        BlockId blockId = new BlockId(blockIdNum);

        return blockManagerServer.getBlock(blockId);
    }

    @Override
    public IBlock newBlockRMI(byte[] b) {
        if (null == b)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_RMI_DATA_ARG);

        BlockManagerId blockManagerId = new BlockManagerId(id.getBlockManagerIdStr());
        IBlockManager blockManagerServer = BlockManagerServer.getServer(blockManagerId);

        return blockManagerServer.newBlock(b);
    }

    @Override
    public String getPathRMI() {
        BlockManagerId blockManagerId = new BlockManagerId(id.getBlockManagerIdStr());
        IBlockManager blockManagerServer = BlockManagerServer.getServer(blockManagerId);

        return blockManagerServer.getPath();
    }
}