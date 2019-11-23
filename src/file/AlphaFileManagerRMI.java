package file;

import constant.ConfigConstants;
import util.ErrorCode;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class AlphaFileManagerRMI extends UnicastRemoteObject implements IFileManagerRMI {
    public AlphaFileManagerRMI() throws RemoteException {
        super();
    }

    @Override
    public IFile getFileRMI(String fileManagerIdStr, String fieldIdStr) {
        if (null == fileManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_MANAGER_ID_ARG);
        if ( null == fieldIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FIELD_ID_ARG);

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(fileManagerIdStr);
        IFileManager fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);

        FieldId fieldId = new FieldId(fieldIdStr);

        return fileManagerServer.getFile(fieldId);
    }

    @Override
    public IFile newFileRMI(String fileManagerIdStr, String fieldIdStr) {
        if (null == fileManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_MANAGER_ID_ARG);
        if ( null == fieldIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FIELD_ID_ARG);

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(fileManagerIdStr);
        IFileManager fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);

        FieldId fieldId = new FieldId(fieldIdStr);

        return fileManagerServer.newFile(fieldId);
    }

    @Override
    public String getPathRMI(String fileManagerIdStr) {
        if (null == fileManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_MANAGER_ID_ARG);

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(fileManagerIdStr);
        IFileManager fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);

        return fileManagerServer.getPath();
    }

    @Override
    public AlphaFile setSizeRMI(String fileManagerIdStr, String fieldIdStr, long newSize) {
        if (null == fileManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_MANAGER_ID_ARG);
        if ( null == fieldIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FIELD_ID_ARG);

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(fileManagerIdStr);
        AlphaFileManagerServer fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);

        FieldId fieldId = new FieldId(fieldIdStr);
        AlphaFile file = fileManagerServer.getFile(fieldId);

        file.setSize(newSize);
        file.close();
        fileManagerServer.updateCache(fieldId, file);
        file.setRemote(ConfigConstants.RMI_SERVER_HOST);

        return file;
    }

    @Override
    public void writeMetaRMI(String fileManagerIdStr, String fieldIdStr, IFile file) {
        if (null == fileManagerIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_MANAGER_ID_ARG);
        if (null == fieldIdStr)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FIELD_ID_ARG);
        if (null == file)
            throw new ErrorCode(ErrorCode.NULL_FILE_RMI_FILE_ARG);
        if (file.getClass() != AlphaFile.class)
            throw new ErrorCode(ErrorCode.INVALID_FILE_RMI_FILE_ARG);

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(fileManagerIdStr);
        AlphaFileManagerServer fileManagerServer = AlphaFileManagerServer.getServer(fileManagerId);
        FieldId fieldId = new FieldId(fieldIdStr);

        AlphaFile alphaFile = (AlphaFile) file;
        alphaFile.setLocal();
        alphaFile.close();
        fileManagerServer.updateCache(fieldId, alphaFile);
    }
}
