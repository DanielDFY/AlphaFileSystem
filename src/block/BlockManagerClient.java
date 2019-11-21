package block;

import constant.ConfigConstants;
import id.Id;
import util.ErrorCode;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMISocketFactory;
import java.util.HashMap;


public class BlockManagerClient implements IBlockManager {
    /* Client static controller */
    private static HashMap<BlockManagerClientId, BlockManagerClient> clientMap = new HashMap<>();

    public static void addClient(BlockManagerClientId blockManagerClientId) {
        if (null == blockManagerClientId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_CLIENT_ID);
        }

        if (clientMap.containsKey(blockManagerClientId)) {
            throw new ErrorCode(ErrorCode.EXISTING_BLOCK_MANAGER_CLIENT_ID, blockManagerClientId.getHostStr() + "/" + blockManagerClientId.getBlockManagerIdStr());
        } else {
            clientMap.put(blockManagerClientId, new BlockManagerClient(blockManagerClientId));
        }
    }

    public static BlockManagerClient getClient(BlockManagerClientId blockManagerClientId) {
        if (null == blockManagerClientId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_CLIENT_ID);
        }

        if (!clientMap.containsKey(blockManagerClientId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_CLIENT_ID, blockManagerClientId.getHostStr() + "/" + blockManagerClientId.getBlockManagerIdStr());
        } else {
            return clientMap.get(blockManagerClientId);
        }
    }

    public static void removeClient(BlockManagerClientId blockManagerClientId) {
        if (null == blockManagerClientId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_CLIENT_ID);
        }

        if (!clientMap.containsKey(blockManagerClientId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_CLIENT_ID, blockManagerClientId.getHostStr() + "/" + blockManagerClientId.getBlockManagerIdStr());
        } else {
            clientMap.remove(blockManagerClientId);
        }
    }

    static {
        try {
            RMISocketFactory.setSocketFactory(new RMISocketFactory()
            {
                public Socket createSocket(String host, int port) throws IOException {
                    Socket socket = new Socket(host, port);
                    socket.setSoTimeout(ConfigConstants.BLOCK_MANAGER_CLIENT_SOCKET_TIMEOUT);
                    socket.setSoLinger(false, 0);
                    return socket;
                }

                public ServerSocket createServerSocket(int port) throws IOException {
                    return new ServerSocket(port);
                }
            });
        } catch (Exception e) {
            throw new ErrorCode(ErrorCode.FAILED_TO_SET_SOCKET_TIMEOUT);
        }
    }

    private final BlockManagerClientId blockManagerClientId;
    private final IBlockManagerRMI blockManagerRMI;
    private final HashMap<BlockId, Block> blockBuffer;

    public BlockManagerClient(BlockManagerClientId blockManagerClientId) {

        this.blockManagerClientId = blockManagerClientId;
        this.blockManagerRMI = connectHost();
        this.blockBuffer = new HashMap<>();
    }

    private IBlockManagerRMI connectHost() {
        try {
            Registry registry = LocateRegistry.getRegistry(blockManagerClientId.getHostStr());
            return (IBlockManagerRMI)registry.lookup(ConfigConstants.RMI_BLOCK_MANAGER_REGISTRY_PREFIX + ConfigConstants.RMI_SERVER_HOST);
        } catch (RemoteException e) {
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_CLIENT_CONNECT_FAILURE, ConfigConstants.RMI_SERVER_HOST);
        } catch (NotBoundException e) {
            throw new ErrorCode(ErrorCode.NOT_BOUND_BLOCK_RMI, ConfigConstants.RMI_BLOCK_MANAGER_REGISTRY_PREFIX);
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
            Block block = (Block) blockManagerRMI.getBlockRMI(blockManagerClientId.getBlockManagerIdStr(), blockClientId.getId());
            updateBuffer(block);
            return block;
        } catch (Exception e) {
            String serverStr = ConfigConstants.RMI_BLOCK_MANAGER_REGISTRY_PREFIX + blockManagerClientId.getHostStr() + "/" + blockManagerClientId.getBlockManagerIdStr() + ": ";
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public Block newBlock(byte[] b) {
        if (null == b)
            throw new ErrorCode(ErrorCode.NULL_NEW_BLOCK_DATA);

        try {
            Block block = (Block) blockManagerRMI.newBlockRMI(blockManagerClientId.getBlockManagerIdStr(), b);
            updateBuffer(block);
            return block;
        } catch (Exception e) {
            String serverStr = ConfigConstants.RMI_BLOCK_MANAGER_REGISTRY_PREFIX + blockManagerClientId.getHostStr() + "/" + blockManagerClientId.getBlockManagerIdStr() + ": ";
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public String getPath() {
        try {
            return ConfigConstants.RMI_BLOCK_MANAGER_REGISTRY_PREFIX + blockManagerClientId.getHostStr() + "/" + blockManagerRMI.getPathRMI(blockManagerClientId.getBlockManagerIdStr());
        } catch (Exception e) {
            String serverStr = ConfigConstants.RMI_BLOCK_MANAGER_REGISTRY_PREFIX + blockManagerClientId.getHostStr() + "/" + blockManagerClientId.getBlockManagerIdStr() + ": ";
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public BlockManagerClientId getManagerId() {
        return blockManagerClientId;
    }
}
