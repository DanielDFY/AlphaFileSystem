package file;

import constant.ConfigConstants;
import util.ErrorCode;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class AlphaFileManagerRMI extends UnicastRemoteObject implements IFileManagerRMI {
    private AlphaFileManagerRMIId id;

    public AlphaFileManagerRMI(AlphaFileManagerRMIId id) throws RemoteException {
        super();
        if (null == id)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_MANAGER_ID_ARG);
        this.id = id;
    }

    @Override
    public IFile getFileRMI(FieldId fieldId) {
        if (null == fieldId)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FIELD_ID_ARG);

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(id.getFileManagerIdStr());
        IFileManager fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);

        AlphaFile file = (AlphaFile) fileManagerServer.getFile(fieldId);
        file.setRemote(ConfigConstants.RMI_SERVER_HOST, ConfigConstants.RMI_SERVER_PORT);

        return file;
    }

    @Override
    public IFile newFileRMI(FieldId fieldId) {
        if (null == fieldId)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FIELD_ID_ARG);

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(id.getFileManagerIdStr());
        IFileManager fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);

        AlphaFile file = (AlphaFile) fileManagerServer.newFile(fieldId);
        file.setRemote(ConfigConstants.RMI_SERVER_HOST, ConfigConstants.RMI_SERVER_PORT);

        return file;
    }

    @Override
    public String getPathRMI() {
        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(id.getFileManagerIdStr());
        IFileManager fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);

        return fileManagerServer.getPath();
    }

    @Override
    public AlphaFile setSizeRMI(FieldId fieldId, long newSize) {
        if ( null == fieldId)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FIELD_ID_ARG);

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(id.getFileManagerIdStr());
        AlphaFileManagerServer fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);

        AlphaFile file = fileManagerServer.getFile(fieldId);
        file.setLocal();
        file.setSize(newSize);
        file.close();
        fileManagerServer.updateCache(fieldId, file);
        file.setRemote(ConfigConstants.RMI_SERVER_HOST, ConfigConstants.RMI_SERVER_PORT);

        return file;
    }

    @Override
    public void writeMetaRMI(FieldId fieldId, IFile file) {
        if (null == fieldId)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FIELD_ID_ARG);
        if (null == file)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FILE_ARG);
        if (file.getClass() != AlphaFile.class)
            throw new ErrorCode(ErrorCode.INVALID_FILE_RMI_FILE_ARG);

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(id.getFileManagerIdStr());
        AlphaFileManagerServer fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);

        AlphaFile alphaFile = (AlphaFile) file;
        alphaFile.setLocal();
        alphaFile.close();
        fileManagerServer.updateCache(fieldId, alphaFile);
    }
}
