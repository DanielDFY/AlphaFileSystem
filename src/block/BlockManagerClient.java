package block;

import constant.ConfigConstants;
import id.Id;
import util.ErrorCode;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;


public class BlockManagerClient implements IBlockManager {
    /* Client static controller */
    private static HashMap<BlockManagerRMIId, BlockManagerClient> clientMap = new HashMap<>();

    public static void listServers() {
        if (clientMap.size() != 0) {
            System.out.println("Block manager client List:");
            for (BlockManagerRMIId id : clientMap.keySet()) {
                System.out.println(id.getHostStr() + "/" + id.getBlockManagerIdStr());
            }
        }
    }

    public static void addClient(BlockManagerRMIId blockManagerRMIId) {
        if (null == blockManagerRMIId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_CLIENT_ID);
        }

        if (clientMap.containsKey(blockManagerRMIId)) {
            throw new ErrorCode(ErrorCode.EXISTING_BLOCK_MANAGER_CLIENT_ID, blockManagerRMIId.getHostStr() + "/" + blockManagerRMIId.getBlockManagerIdStr());
        } else {
            clientMap.put(blockManagerRMIId, new BlockManagerClient(blockManagerRMIId));
        }
    }

    public static BlockManagerClient getClient(BlockManagerRMIId blockManagerRMIId) {
        if (null == blockManagerRMIId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_CLIENT_ID);
        }

        if (!clientMap.containsKey(blockManagerRMIId)) {
            addClient(blockManagerRMIId);
        }
        return clientMap.get(blockManagerRMIId);
    }

    public static void removeClient(BlockManagerRMIId blockManagerRMIId) {
        if (null == blockManagerRMIId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_CLIENT_ID);
        }

        if (!clientMap.containsKey(blockManagerRMIId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_CLIENT_ID, blockManagerRMIId.getHostStr() + "/" + blockManagerRMIId.getBlockManagerIdStr());
        } else {
            clientMap.remove(blockManagerRMIId);
        }
    }

    private final BlockManagerRMIId blockManagerRMIId;
    private final IBlockManagerRMI blockManagerRMI;
    private final HashMap<BlockId, Block> blockBuffer;

    public BlockManagerClient(BlockManagerRMIId blockManagerRMIId) {

        this.blockManagerRMIId = blockManagerRMIId;
        this.blockManagerRMI = connectHost();
        this.blockBuffer = new HashMap<>();
    }

    private IBlockManagerRMI connectHost() {
        try {
            return (IBlockManagerRMI) Naming.lookup(blockManagerRMIId.toString());
        } catch (RemoteException e) {
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_CLIENT_CONNECT_FAILURE, ConfigConstants.RMI_SERVER_HOST);
        } catch (NotBoundException | MalformedURLException e) {
            throw new ErrorCode(ErrorCode.NOT_BOUND_BLOCK_RMI, ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX);
        }
    }

    private void updateBuffer(Block block) {
        // hit
        if (blockBuffer.containsKey(block.getIndexId()))
            return;

        // miss
        if (blockBuffer.size() == ConfigConstants.BLOCK_MANAGER_CLIENT_BUFFER_SIZE) {
            int random = (int) (Math.random() * blockBuffer.size());
            for (BlockId blockId : blockBuffer.keySet()) {
                if (random == 0) {
                    blockBuffer.remove(blockId);
                } else {
                    --random;
                }
            }
        }

        blockBuffer.put(block.getIndexId(), block);
    }

    @Override
    public Block getBlock(Id indexId) {
        if (null == indexId)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_ID_ARG);

        if (indexId.getClass() != BlockClientId.class)
            throw new ErrorCode(ErrorCode.BLOCK_CLIENT_INVALID_BLOCK_TYPE);

        try {
            BlockClientId blockClientId = (BlockClientId) indexId;
            Block block = (Block) blockManagerRMI.getBlockRMI(blockClientId.getId());
            updateBuffer(block);
            return block;
        } catch (Exception e) {
            String serverStr = blockManagerRMIId.toString() + ": ";
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public Block newBlock(byte[] b) {
        if (null == b)
            throw new ErrorCode(ErrorCode.NULL_NEW_BLOCK_DATA);

        try {
            Block block = (Block) blockManagerRMI.newBlockRMI(b);
            updateBuffer(block);
            return block;
        } catch (Exception e) {
            String serverStr = blockManagerRMIId.toString() + ": ";
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public String getPath() {
        try {
            return ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + blockManagerRMIId.getHostStr() + "/" + blockManagerRMI.getPathRMI();
        } catch (Exception e) {
            String serverStr = blockManagerRMIId.toString() + ": ";
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public BlockManagerRMIId getManagerId() {
        return blockManagerRMIId;
    }
}
