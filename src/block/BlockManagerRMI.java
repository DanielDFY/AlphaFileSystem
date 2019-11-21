package block;

import util.ErrorCode;

import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class BlockManagerRMI extends UnicastRemoteObject implements IBlockManagerRMI {
    public BlockManagerRMI() throws RemoteException {
        super();
    }

    @Override
    public IBlock getBlockRMI(String blockManagerIdStr, long blockIdNum) {
        if (null == blockManagerIdStr || 0 == blockIdNum)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_RMI_ID_ARG);

        BlockManagerId blockManagerId = new BlockManagerId(blockManagerIdStr);
        IBlockManager blockManagerServer = BlockManagerServer.getServer(blockManagerId);

        BlockId blockId = new BlockId(blockIdNum);

        return blockManagerServer.getBlock(blockId);
    }

    @Override
    public IBlock newBlockRMI(String blockManagerIdStr, byte[] b) {
        if (null == b)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_RMI_DATA_ARG);

        if (null == blockManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_RMI_ID_ARG);

        BlockManagerId blockManagerId = new BlockManagerId(blockManagerIdStr);
        IBlockManager blockManagerServer = BlockManagerServer.getServer(blockManagerId);

        return blockManagerServer.newBlock(b);
    }

    @Override
    public String getPathRMI(String blockManagerIdStr) {
        if (null == blockManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_RMI_ID_ARG);

        BlockManagerId blockManagerId = new BlockManagerId(blockManagerIdStr);
        IBlockManager blockManagerServer = BlockManagerServer.getServer(blockManagerId);

        return blockManagerServer.getPath();
    }

    @Override
    public void terminate() throws RemoteException {
        try {
            UnicastRemoteObject.unexportObject(this, true);
        } catch (NoSuchObjectException e) {
            throw new RemoteException(e.getMessage());
        }
    }
}