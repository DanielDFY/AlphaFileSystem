package file;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IFileManagerRMI extends Remote {
    IFile getFileRMI(FieldId fieldId) throws RemoteException;
    IFile newFileRMI(FieldId fieldId) throws RemoteException;
    String getPathRMI() throws RemoteException;
    IFile setSizeRMI(FieldId fieldId, long newSize) throws RemoteException;
    void writeMetaRMI(FieldId fieldId, IFile file) throws RemoteException;
}
