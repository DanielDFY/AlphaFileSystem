package block;

import constant.ConfigConstants;
import constant.PathConstants;
import id.Id;
import util.ByteUtils;
import util.ErrorCode;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

public class BlockManagerServer implements IBlockManager {
    /* Server static controller */
    private static HashMap<BlockManagerId, Boolean> switchMap = new HashMap<>();
    private static HashMap<BlockManagerId, BlockManagerServer> serverMap = new HashMap<>();

    public static void listServers() {
        if (noServing()) {
            System.out.println("No block manager serving");
        }
        else {
            System.out.println("Block manager server list:");
            for (BlockManagerId id : switchMap.keySet()) {
                System.out.println(id.getId() + " : " + switchMap.get(id));
            }
        }
    }

    public static void startManager(BlockManagerId blockManagerId) {
        if (null == blockManagerId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_SERVER_ID);
        }
        if (!switchMap.containsKey(blockManagerId) || !serverMap.containsKey(blockManagerId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_SERVER_ID, blockManagerId.getId());
        }

        BlockManagerServer blockManagerServer = new BlockManagerServer(blockManagerId);
        blockManagerServer.launchRMI();

        switchMap.replace(blockManagerId, true);
        serverMap.replace(blockManagerId, blockManagerServer);
    }

    public static void stopManager(BlockManagerId blockManagerId) {
        if (null == blockManagerId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_SERVER_ID);
        }
        if (!switchMap.containsKey(blockManagerId) || !serverMap.containsKey(blockManagerId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_SERVER_ID, blockManagerId.getId());
        }

        BlockManagerServer blockManagerServer = serverMap.get(blockManagerId);
        if (null != blockManagerServer)
            blockManagerServer.terminateRMI();

        switchMap.replace(blockManagerId, false);
    }

    public static void restartManager(BlockManagerId blockManagerId) throws ErrorCode {
        try {
            stopManager(blockManagerId);
            startManager(blockManagerId);
        } catch (Exception e) {
            stopManager(blockManagerId);
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_SERVER_EXCEPTION, blockManagerId.getId());
        }
    }

    public static void startAllManager() {
        for (BlockManagerId blockManagerId : switchMap.keySet()) {
            startManager(blockManagerId);
        }
    }

    public static void stopAllManager() {
        for (BlockManagerId blockManagerId : switchMap.keySet()) {
            stopManager(blockManagerId);
        }
    }

    public static boolean isServing(BlockManagerId blockManagerId) {
        if (null == blockManagerId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_SERVER_ID);
        }

        if (switchMap.containsKey(blockManagerId)) {
            return switchMap.get(blockManagerId);
        } else {
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_SERVER_ID, blockManagerId.getId());
        }
    }

    public static boolean noServing() {
        for (BlockManagerId blockManagerId : switchMap.keySet()) {
            if (switchMap.get(blockManagerId))
                return false;   // exist serving manager
        }
        return true;    // No serving manager
    }

    public static int serving() {
        int count = 0;
        for (BlockManagerId blockManagerId : switchMap.keySet()) {
            if (switchMap.get(blockManagerId))
                ++count;
        }
        return count;
    }

    public static void addServer(BlockManagerId blockManagerId) {
        if (null == blockManagerId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_SERVER_ID);
        }

        if (switchMap.containsKey(blockManagerId)) {
            throw new ErrorCode(ErrorCode.EXISTING_BLOCK_MANAGER_SERVER_ID, blockManagerId.getId());
        } else {
            switchMap.put(blockManagerId, false);
            serverMap.put(blockManagerId, null);
        }
    }

    public static BlockManagerServer getServer(BlockManagerId blockManagerId) {
        if (null == blockManagerId) {
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_SERVER_ID);
        }

        if (!switchMap.containsKey(blockManagerId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_SERVER_ID, blockManagerId.getId());
        } else {
            return serverMap.get(blockManagerId);
        }
    }

    /* Initialize local block managers */
    public static void init() {
        File blockManagerDir = new File(PathConstants.BLOCK_MANAGER_PATH);
        if (blockManagerDir.exists()) {
            for (int i = 1; i <= ConfigConstants.BLOCK_MANAGER_NUM; ++i) {
                BlockManagerId blockManagerId = new BlockManagerId(PathConstants.BLOCK_MANAGER_PREFIX + i);
                addServer(blockManagerId);
            }
            return;
        } else if (!blockManagerDir.mkdir()) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, blockManagerDir.getPath());
        }

        File blockCount = new File(PathConstants.BLOCK_MANAGER_PATH, PathConstants.BLOCK_ID_COUNT);
        try {
            RandomAccessFile output = new RandomAccessFile(blockCount, "rwd");
            output.write(ByteUtils.longToBytes(0));
            output.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, blockCount.getPath());
        }

        File blockManagerCount = new File(PathConstants.BLOCK_MANAGER_PATH, PathConstants.BLOCK_MANAGER_ID_COUNT);
        try {
            RandomAccessFile output = new RandomAccessFile(blockManagerCount, "rwd");
            output.write(ByteUtils.longToBytes(0));
            output.close();
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, blockManagerCount.getPath());
        }

        for (int i = 0; i < ConfigConstants.BLOCK_MANAGER_NUM; ++i) {
            BlockManagerServer blockManagerServer = new BlockManagerServer();
            addServer(blockManagerServer.getManagerId());
        }
    }

    /* static helper method */
    // randomly choose an serving block manager
    public static BlockManager getRandomServingBlockManager() {
        if (noServing()) {
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_NO_SERVING);
        }

        if (serving() < ConfigConstants.DUPLICATION_NUM) {
            throw new ErrorCode(ErrorCode.LACKING_SERVER_FOR_DUPLICATION);
        }

        BlockManagerId randomBlockManagerId;
        do {
            long randomBlockManagerIdNum = (long) (Math.random() * switchMap.size()) + 1; // + 1 for id number starts from 1
            randomBlockManagerId = new BlockManagerId(PathConstants.BLOCK_MANAGER_PREFIX + randomBlockManagerIdNum);
        } while (!isServing(randomBlockManagerId));
        return new BlockManager(randomBlockManagerId);
    }

    /* Server object */
    private final BlockManagerId blockManagerId;
    private IBlockManagerRMI blockManagerRMI;
    private final HashMap<BlockId, Block> blockCache;

    // create new block manager server
    public BlockManagerServer() {
        this.blockManagerId = new BlockManager().getManagerId();
        this.blockCache = new HashMap<>();
    }

    // get existing block manager server
    public BlockManagerServer(BlockManagerId blockManagerId) {
        if (null == blockManagerId)
            throw new ErrorCode(ErrorCode.NULL_BLOCK_MANAGER_ARGUMENT);

        this.blockManagerId = new BlockManager(blockManagerId).getManagerId();
        this.blockCache = new HashMap<>();
    }

    private void updateCache(Block block) {
        // hit
        if (blockCache.containsKey(block.getIndexId()))
            return;

        // miss
        if (blockCache.size() == ConfigConstants.BLOCK_MANAGER_SERVER_CACHE_SIZE) {
            int random = (int) (Math.random() * blockCache.size());
            for (BlockId blockId : blockCache.keySet()) {
                if (random == 0) {
                    blockCache.remove(blockId);
                } else {
                    --random;
                }
            }
        }
        blockCache.put(block.getIndexId(), block);
    }

    // launch block manager RMI service for other clients
    private void launchRMI() {
        try {
            BlockManagerRMIId blockManagerRMIId = new BlockManagerRMIId(ConfigConstants.RMI_SERVER_HOST, ConfigConstants.RMI_SERVER_PORT, blockManagerId.getId());
            blockManagerRMI = new BlockManagerRMI(blockManagerRMIId);
            Naming.rebind(blockManagerRMIId.toString(), blockManagerRMI);
        } catch (RemoteException e) {
            throw new ErrorCode(ErrorCode.MANAGER_REGISTRY_LAUNCH_FAILURE);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            throw new ErrorCode(ErrorCode.BOUND_BLOCK_RMI_ERROR);
        }
    }

    public void terminateRMI() {
        String bindName = ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + ConfigConstants.RMI_SERVER_HOST + ":" + ConfigConstants.RMI_SERVER_PORT + "/" + blockManagerId.getId();
        try {
            UnicastRemoteObject.unexportObject(blockManagerRMI, true);
            Naming.unbind(bindName);
        } catch (NoSuchObjectException | NotBoundException e) {
            throw new ErrorCode(ErrorCode.TERMINATE_UNKNOWN_BLOCK_RMI);
        } catch (RemoteException | MalformedURLException e) {
            throw new ErrorCode(ErrorCode.TERMINATE_BLOCK_RMI_ERROR, bindName);
        }
    }

    @Override
    public Block getBlock(Id indexId) {
        if (!BlockManagerServer.isServing(blockManagerId)) {
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_NOT_SERVING, blockManagerId.getId());
        }

        // hit
        if (blockCache.containsKey(indexId))
            return blockCache.get(indexId);

        // miss
        try {
            BlockManager blockManager = new BlockManager(blockManagerId);
            Block block = blockManager.getBlock(indexId);
            updateCache(block);
            return block;
        } catch (ErrorCode errorCode) {
            throw errorCode;
        } catch (Exception e) {
            restartManager(blockManagerId);
            throw new ErrorCode(ErrorCode.RESTART_BLOCK_MANAGER_SERVER_EXCEPTION, blockManagerId.getId());
        }
    }

    @Override
    public Block newBlock(byte[] b) {
        if (!BlockManagerServer.isServing(blockManagerId)) {
            throw new ErrorCode(ErrorCode.BLOCK_MANAGER_NOT_SERVING, blockManagerId.getId());
        }

        try {
            BlockManager blockManager = new BlockManager(blockManagerId);
            Block block = blockManager.newBlock(b);
            updateCache(block);
            return block;
        } catch (ErrorCode errorCode) {
            throw errorCode;
        } catch (Exception e) {
            stopManager(blockManagerId);
            throw new ErrorCode(ErrorCode.UNKNOWN_BLOCK_MANAGER_SERVER_EXCEPTION, blockManagerId.getId());
        }
    }

    @Override
    public String getPath() {
        return PathConstants.BLOCK_MANAGER_PATH + "/" + blockManagerId.getId();
    }

    @Override
    public BlockManagerId getManagerId() {
        return blockManagerId;
    }
}
