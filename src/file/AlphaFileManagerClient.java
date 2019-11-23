package file;

import constant.ConfigConstants;
import id.Id;
import util.ErrorCode;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashMap;

public class AlphaFileManagerClient implements IFileManager {
    /* Client static controller */
    private static HashMap<AlphaFileManagerClientId, AlphaFileManagerClient> clientMap = new HashMap<>();

    public static void addClient(AlphaFileManagerClientId fileManagerClientId) {
        if (null == fileManagerClientId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CLIENT_ID);
        }

        if (clientMap.containsKey(fileManagerClientId)) {
            throw new ErrorCode(ErrorCode.EXISTING_FILE_MANAGER_CLIENT_ID, fileManagerClientId.getHostStr() + "/" + fileManagerClientId.getFileManagerIdStr());
        } else {
            clientMap.put(fileManagerClientId, new AlphaFileManagerClient(fileManagerClientId));
        }
    }

    public static AlphaFileManagerClient getClient(AlphaFileManagerClientId fileManagerClientId) {
        if (null == fileManagerClientId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CLIENT_ID);
        }

        if (!clientMap.containsKey(fileManagerClientId)) {
            addClient(fileManagerClientId);
        }
        return clientMap.get(fileManagerClientId);
    }

    public static void removeClient(AlphaFileManagerClientId fileManagerClientId) {
        if (null == fileManagerClientId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CLIENT_ID);
        }

        if (!clientMap.containsKey(fileManagerClientId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_CLIENT_ID, fileManagerClientId.getHostStr() + "/" + fileManagerClientId.getFileManagerIdStr());
        } else {
            clientMap.remove(fileManagerClientId);
        }
    }

    private final AlphaFileManagerClientId fileManagerClientId;
    private final IFileManagerRMI fileManagerRMI;

    public AlphaFileManagerClient(AlphaFileManagerClientId fileManagerClientId) {
        this.fileManagerClientId = fileManagerClientId;
        this.fileManagerRMI = connectHost();
    }

    private IFileManagerRMI connectHost() {
        try {
            Registry registry = LocateRegistry.getRegistry(fileManagerClientId.getHostStr());
            return (IFileManagerRMI)registry.lookup(ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + ConfigConstants.RMI_SERVER_HOST + ConfigConstants.FILE_RMI_SERVER_PREFIX);
        } catch (RemoteException e) {
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_CONNECT_FAILURE, ConfigConstants.RMI_SERVER_HOST);
        } catch (NotBoundException e) {
            throw new ErrorCode(ErrorCode.NOT_BOUND_FILE_RMI, ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX);
        }
    }

    @Override
    public AlphaFile getFile(Id id) {
        if (null == id)
            throw new ErrorCode(ErrorCode.NULL_FIELD_ID);

        if (id.getClass() != FieldId.class)
            throw new ErrorCode(ErrorCode.FILE_CLIENT_INVALID_FIELD_TYPE);

        try {
            FieldId fieldId = (FieldId) id;
            return (AlphaFile) fileManagerRMI.getFileRMI(fileManagerClientId.getFileManagerIdStr(), fieldId.getId());
        } catch (Exception e) {
            String serverStr = ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + fileManagerClientId.getHostStr() + "/" + fileManagerClientId.getFileManagerIdStr() + ": ";
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public AlphaFile newFile(Id id) {
        if (null == id)
            throw new ErrorCode(ErrorCode.NULL_FIELD_ID);

        if (id.getClass() != FieldId.class)
            throw new ErrorCode(ErrorCode.FILE_CLIENT_INVALID_FIELD_TYPE);

        try {
            FieldId fieldId = (FieldId) id;
            return (AlphaFile) fileManagerRMI.newFileRMI(fileManagerClientId.getFileManagerIdStr(), fieldId.getId());
        } catch (Exception e) {
            String serverStr = ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + fileManagerClientId.getHostStr() + "/" + fileManagerClientId.getFileManagerIdStr() + ": ";
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public String getPath() {
        try {
            return ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + fileManagerClientId.getHostStr() + "/" + fileManagerRMI.getPathRMI(fileManagerClientId.getFileManagerIdStr());
        } catch (Exception e) {
            String serverStr = ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + fileManagerClientId.getHostStr() + "/" + fileManagerClientId.getFileManagerIdStr() + ": ";
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public AlphaFileManagerClientId getManagerId() {
        return fileManagerClientId;
    }

    public AlphaFile setRemoteFileSize(FieldId fieldId, long newSize) {
        try {
            return (AlphaFile) fileManagerRMI.setSizeRMI(fileManagerClientId.getFileManagerIdStr(), fieldId.getId(), newSize);
        } catch (Exception e) {
            String serverStr = ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + fileManagerClientId.getHostStr() + "/" + fileManagerClientId.getFileManagerIdStr() + ": ";
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    public void writeRemoteFileMeta(FieldId fieldId, IFile file) {
        try {
            fileManagerRMI.writeMetaRMI(fileManagerClientId.getFileManagerIdStr(), fieldId.getId(), file);
        } catch (Exception e) {
            String serverStr = ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + fileManagerClientId.getHostStr() + "/" + fileManagerClientId.getFileManagerIdStr() + ": ";
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }
}
