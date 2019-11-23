package file;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IFileManagerRMI extends Remote {
    IFile getFileRMI(String fileManagerIdStr, String fieldIdStr) throws RemoteException;
    IFile newFileRMI(String fileManagerIdStr, String fieldIdStr) throws RemoteException;
    String getPathRMI(String fileManagerIdStr) throws RemoteException;
    IFile setSizeRMI(String fileManagerIdStr, String fieldIdStr, long newSize) throws RemoteException;
    void writeMetaRMI(String fileManagerIdStr, String fieldIdStr, IFile file) throws RemoteException;
}
