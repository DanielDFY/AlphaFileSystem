package file;

import constant.ConfigConstants;
import id.Id;
import util.ErrorCode;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.HashMap;

public class AlphaFileManagerClient implements IFileManager {
    /* Client static controller */
    private static HashMap<AlphaFileManagerRMIId, AlphaFileManagerClient> clientMap = new HashMap<>();

    public static void listServers() {
        if (clientMap.size() != 0) {
            System.out.println("File manager client List:");
            for (AlphaFileManagerRMIId id : clientMap.keySet()) {
                System.out.println(id.getHostStr() + "/" + id.getFileManagerIdStr());
            }
        }
    }

    public static void addClient(AlphaFileManagerRMIId fileManagerClientId) {
        if (null == fileManagerClientId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CLIENT_ID);
        }

        if (clientMap.containsKey(fileManagerClientId)) {
            throw new ErrorCode(ErrorCode.EXISTING_FILE_MANAGER_CLIENT_ID, fileManagerClientId.getHostStr() + "/" + fileManagerClientId.getFileManagerIdStr());
        } else {
            clientMap.put(fileManagerClientId, new AlphaFileManagerClient(fileManagerClientId));
        }
    }

    public static AlphaFileManagerClient getClient(AlphaFileManagerRMIId fileManagerClientId) {
        if (null == fileManagerClientId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CLIENT_ID);
        }

        if (!clientMap.containsKey(fileManagerClientId)) {
            addClient(fileManagerClientId);
        }
        return clientMap.get(fileManagerClientId);
    }

    public static void removeClient(AlphaFileManagerRMIId fileManagerClientId) {
        if (null == fileManagerClientId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CLIENT_ID);
        }

        if (!clientMap.containsKey(fileManagerClientId)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_CLIENT_ID, fileManagerClientId.getHostStr() + "/" + fileManagerClientId.getFileManagerIdStr());
        } else {
            clientMap.remove(fileManagerClientId);
        }
    }

    private final AlphaFileManagerRMIId fileManagerRMIId;
    private final IFileManagerRMI fileManagerRMI;

    public AlphaFileManagerClient(AlphaFileManagerRMIId fileManagerRMIId) {
        this.fileManagerRMIId = fileManagerRMIId;
        this.fileManagerRMI = connectHost();
    }

    private IFileManagerRMI connectHost() {
        try {
            return (IFileManagerRMI) Naming.lookup(fileManagerRMIId.toString());
        } catch (RemoteException e) {
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_CONNECT_FAILURE, ConfigConstants.RMI_SERVER_HOST);
        } catch (NotBoundException | MalformedURLException e) {
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
            AlphaFile file = (AlphaFile) fileManagerRMI.getFileRMI(fieldId);
            file.setRemote(fileManagerRMIId.getHostStr(), fileManagerRMIId.getPort());
            return file;
        } catch (Exception e) {
            String serverStr = fileManagerRMIId.toString() + ": ";
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
            AlphaFile file = (AlphaFile) fileManagerRMI.newFileRMI(fieldId);
            file.setRemote(fileManagerRMIId.getHostStr(), fileManagerRMIId.getPort());
            return file;
        } catch (Exception e) {
            String serverStr = fileManagerRMIId.toString() + ": ";
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public String getPath() {
        try {
            return ConfigConstants.RMI_MANAGER_REGISTRY_PREFIX + fileManagerRMIId.getHostStr() + "/" + fileManagerRMI.getPathRMI();
        } catch (Exception e) {
            String serverStr = fileManagerRMIId.toString() + ": ";
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    @Override
    public AlphaFileManagerRMIId getManagerId() {
        return fileManagerRMIId;
    }

    public AlphaFile setRemoteFileSize(FieldId fieldId, long newSize) {
        try {
            AlphaFile file = (AlphaFile) fileManagerRMI.setSizeRMI(fieldId, newSize);
            file.setRemote(fileManagerRMIId.getHostStr(), fileManagerRMIId.getPort());
            return file;
        } catch (Exception e) {
            String serverStr = fileManagerRMIId.toString() + ": ";
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }

    public void writeRemoteFileMeta(FieldId fieldId, IFile file) {
        try {
            fileManagerRMI.writeMetaRMI(fieldId, file);
        } catch (Exception e) {
            String serverStr = fileManagerRMIId.toString() + ": ";
            throw new ErrorCode(ErrorCode.FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, serverStr + e.getMessage());
        }
    }
}
