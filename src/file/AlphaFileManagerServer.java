package file;

import constant.ConfigConstants;
import constant.PathConstants;
import id.Id;
import util.ByteUtils;
import util.ErrorCode;

import java.io.File;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NoSuchObjectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;

public class AlphaFileManagerServer implements IFileManager {
    /* Server static controller */
    private static HashMap<AlphaFileManagerId, Boolean> switchMap = new HashMap<>();
    private static HashMap<AlphaFileManagerId, AlphaFileManagerServer> serverMap = new HashMap<>();

    public static void listServers() {
        if (noServing()) {
            System.out.println("No file manager server serving");
        }
        else {
            System.out.println("File manager server List:");
            for (AlphaFileManagerId id : switchMap.keySet()) {
                System.out.println(id.getId() + " : " + switchMap.get(id));
            }
        }
    }

    public static void startManager(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_SERVER_ID);
        }
        if (!switchMap.containsKey(fileManagerId) || !serverMap.containsKey(fileManagerId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_SERVER_ID, fileManagerId.getId());
        }

        AlphaFileManagerServer fileManagerServer = new AlphaFileManagerServer(fileManagerId);
        fileManagerServer.launchRMI();

        switchMap.replace(fileManagerId, true);
        serverMap.replace(fileManagerId, fileManagerServer);
    }

    public static void stopManager(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_SERVER_ID);
        }
        if (!switchMap.containsKey(fileManagerId) || !serverMap.containsKey(fileManagerId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_SERVER_ID, fileManagerId.getId());
        }

        AlphaFileManagerServer fileManagerServer = serverMap.get(fileManagerId);
        if (null != fileManagerServer)
            fileManagerServer.terminateRMI();

        switchMap.replace(fileManagerId, false);
    }

    public static void restartManager(AlphaFileManagerId fileManagerId) throws ErrorCode {
        try {
            stopManager(fileManagerId);
            startManager(fileManagerId);
        } catch (Exception e) {
            stopManager(fileManagerId);
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_SERVER_EXCEPTION, fileManagerId.getId());
        }
    }

    public static void startAllManager() {
        for (AlphaFileManagerId fileManagerId : switchMap.keySet()) {
            startManager(fileManagerId);
        }
    }

    public static void stopAllManager() {
        for (AlphaFileManagerId fileManagerId : switchMap.keySet()) {
            stopManager(fileManagerId);
        }
    }

    public static boolean isServing(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_SERVER_ID);
        }

        if (switchMap.containsKey(fileManagerId)) {
            return switchMap.get(fileManagerId);
        } else {
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_SERVER_ID, fileManagerId.getId());
        }
    }

    public static boolean noServing() {
        for (AlphaFileManagerId fileManagerId : switchMap.keySet()) {
            if (switchMap.get(fileManagerId))
                return false;   // exist serving manager
        }
        return true;    // No serving manager
    }

    public static int serving() {
        int count = 0;
        for (AlphaFileManagerId fileManagerId : switchMap.keySet()) {
            if (switchMap.get(fileManagerId))
                ++count;
        }
        return count;
    }

    public static void addServer(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_SERVER_ID);
        }

        if (switchMap.containsKey(fileManagerId)) {
            throw new ErrorCode(ErrorCode.EXISTING_FILE_MANAGER_SERVER_ID, fileManagerId.getId());
        } else {
            switchMap.put(fileManagerId, false);
            serverMap.put(fileManagerId, null);
        }
    }

    public static AlphaFileManagerServer getServer(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_SERVER_ID);
        }

        if (!switchMap.containsKey(fileManagerId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_SERVER_ID, fileManagerId.getId());
        } else {
            return serverMap.get(fileManagerId);
        }
    }

    /* Initialize local file managers */
    public static void init() {
        File fileManagerDir = new File(PathConstants.FILE_MANAGER_PATH);
        if (fileManagerDir.exists()) {
            for (int i = 1; i <= ConfigConstants.FILE_MANAGER_NUM; ++i) {
                AlphaFileManagerId fileManagerId = new AlphaFileManagerId(PathConstants.FILE_MANAGER_PREFIX + i);
                addServer(fileManagerId);
            }
            return;
        } else if (!fileManagerDir.mkdir()) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, fileManagerDir.getPath());
        }

        File fileCount = new File(PathConstants.FILE_MANAGER_PATH, PathConstants.FILE_ID_COUNT);
        try {
            RandomAccessFile output = new RandomAccessFile(fileCount, "rwd");
            output.write(ByteUtils.longToBytes(0));
            output.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        File fileManagerCount = new File(PathConstants.FILE_MANAGER_PATH, PathConstants.FILE_MANAGER_ID_COUNT);
        try {
            RandomAccessFile output = new RandomAccessFile(fileManagerCount, "rwd");
            output.write(ByteUtils.longToBytes(0));
            output.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        for (int i = 0; i < ConfigConstants.FILE_MANAGER_NUM; ++i) {
            AlphaFileManagerServer fileManagerServer = new AlphaFileManagerServer();
            addServer(fileManagerServer.getManagerId());
        }
    }

    /* Server object */
    private final AlphaFileManagerId fileManagerId;
    private IFileManagerRMI fileManagerRMI;
    private final HashMap<Id, AlphaFile> fileCache;

    // create new file manager server
    public AlphaFileManagerServer() {
        this.fileManagerId = new AlphaFileManager().getManagerId();
        this.fileCache = new HashMap<>();
    }

    // get existing block manager server
    public AlphaFileManagerServer(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId)
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_ARGUMENT);

        this.fileManagerId = new AlphaFileManager(fileManagerId).getManagerId();
        this.fileCache = new HashMap<>();
    }

    public void updateCache(Id fieldId, AlphaFile file) {
        // hit or remote update
        if (fileCache.containsKey(fieldId)) {
            fileCache.replace(fieldId, file);
            return;
        }

        // miss
        if (fileCache.size() == ConfigConstants.FILE_MANAGER_SERVER_CACHE_SIZE) {
            int random = (int) (Math.random() * fileCache.size());
            for (Id id : fileCache.keySet()) {
                if (random == 0) {
                    fileCache.remove(id);
                } else {
                    --random;
                }
            }
        }
        fileCache.put(fieldId, file);
    }

    // launch file manager RMI service for other clients
    private void launchRMI() {
        try {
            AlphaFileManagerRMIId fileManagerRMIId = new AlphaFileManagerRMIId(ConfigConstants.RMI_SERVER_HOST, ConfigConstants.RMI_SERVER_PORT, fileManagerId.getId());
            fileManagerRMI = new AlphaFileManagerRMI(fileManagerRMIId);
            Naming.rebind(fileManagerRMIId.toString(), fileManagerRMI);
        } catch (RemoteException e) {
            throw new ErrorCode(ErrorCode.FILE_MANAGER_SERVER_LAUNCH_FAILURE);
        } catch (MalformedURLException e) {
            throw new ErrorCode(ErrorCode.BOUND_FILE_RMI_ERROR);
        }
    }

    public void terminateRMI() {
        try {
            UnicastRemoteObject.unexportObject(fileManagerRMI, true);
            String bindName = ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + ConfigConstants.RMI_SERVER_HOST + ":" + ConfigConstants.RMI_SERVER_PORT + "/" + fileManagerId.getId();
            Naming.unbind(bindName);
        } catch (NoSuchObjectException | NotBoundException e) {
            throw new ErrorCode(ErrorCode.TERMINATE_UNKNOWN_FILE_RMI);
        } catch (RemoteException | MalformedURLException e) {
            throw new ErrorCode(ErrorCode.TERMINATE_FILE_RMI_ERROR);
        }
    }

    @Override
    public AlphaFile getFile(Id fieldId) {
        if (!AlphaFileManagerServer.isServing(fileManagerId))
            throw new ErrorCode(ErrorCode.FILE_MANAGER_NOT_SERVING, fileManagerId.getId());

        // hit
        if (fileCache.containsKey(fieldId))
            return fileCache.get(fieldId);

        // miss
        try {
            AlphaFileManager fileManager = new AlphaFileManager(fileManagerId);
            AlphaFile file = fileManager.getFile(fieldId);
            updateCache(fieldId, file);
            return file;
        } catch (ErrorCode errorCode) {
            throw errorCode;
        } catch (Exception e) {
            restartManager(fileManagerId);
            throw new ErrorCode(ErrorCode.RESTART_FILE_MANAGER_SERVER_EXCEPTION, fileManagerId.getId());
        }
    }

    @Override
    public AlphaFile newFile(Id fieldId) {
        if (!AlphaFileManagerServer.isServing(fileManagerId))
            throw new ErrorCode(ErrorCode.FILE_MANAGER_NOT_SERVING, fileManagerId.getId());

        try {
            AlphaFileManager fileManager = new AlphaFileManager(fileManagerId);
            AlphaFile file = fileManager.newFile(fieldId);
            updateCache(fieldId, file);
            return file;
        } catch (ErrorCode errorCode) {
            throw errorCode;
        } catch (Exception e) {
            restartManager(fileManagerId);
            throw new ErrorCode(ErrorCode.RESTART_FILE_MANAGER_SERVER_EXCEPTION, fileManagerId.getId());
        }
    }

    @Override
    public String getPath() {
        AlphaFileManager fileManager = new AlphaFileManager(fileManagerId);
        return fileManager.getPath();
    }

    @Override
    public AlphaFileManagerId getManagerId() {
        return fileManagerId;
    }
}
