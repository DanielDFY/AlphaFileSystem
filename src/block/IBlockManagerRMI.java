package block;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IBlockManagerRMI extends Remote {
    IBlock getBlockRMI(long blockIdNum) throws RemoteException;
    IBlock newBlockRMI(byte[] b) throws RemoteException;
    String getPathRMI() throws RemoteException;
}