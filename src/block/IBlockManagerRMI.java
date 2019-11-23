package block;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface IBlockManagerRMI extends Remote {
    IBlock getBlockRMI(String blockManagerIdStr, long blockIdNum) throws RemoteException;
    IBlock newBlockRMI(String blockManagerIdStr, byte[] b) throws RemoteException;
    String getPathRMI(String blockManagerIdStr) throws RemoteException;
}