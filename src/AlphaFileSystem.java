import block.Block;
import block.BlockClientId;
import block.BlockId;
import block.BlockManagerClient;
import block.BlockManagerRMIId;
import block.BlockManagerId;
import block.BlockManagerServer;
import constant.ConfigConstants;
import constant.PathConstants;
import file.AlphaFileManagerClient;
import file.AlphaFileManagerRMIId;
import file.AlphaFileManagerId;
import file.AlphaFileManagerServer;
import file.FieldId;
import file.IFile;
import file.IFileManager;
import util.ByteUtils;
import util.ErrorCode;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.RMISocketFactory;
import java.util.Scanner;

public class AlphaFileSystem {
    static {
        checkConfig();

        try {
            RMISocketFactory.setSocketFactory(new RMISocketFactory()
            {
                public Socket createSocket(String host, int port) throws IOException {
                    Socket socket = new Socket(host, port);
                    socket.setSoTimeout(ConfigConstants.MANAGER_CLIENT_SOCKET_TIMEOUT);
                    socket.setSoLinger(false, 0);
                    return socket;
                }

                public ServerSocket createServerSocket(int port) throws IOException {
                    return new ServerSocket(port);
                }
            });
        } catch (Exception e) {
            throw new ErrorCode(ErrorCode.FAILED_TO_SET_SOCKET_TIMEOUT);
        }

        try {
            LocateRegistry.createRegistry(ConfigConstants.RMI_SERVER_PORT);
        } catch (RemoteException e) {
            throw new ErrorCode(ErrorCode.MANAGER_REGISTRY_LAUNCH_FAILURE);
        }

        File systemDir = new File(PathConstants.SystemPath);
        if (!systemDir.exists()) {
            if (!systemDir.mkdir()) {
                throw new ErrorCode(ErrorCode.IO_EXCEPTION, systemDir.getPath());
            }
        }

        // init server
        BlockManagerServer.init();
        BlockManagerServer.startAllManager();
        AlphaFileManagerServer.init();
        AlphaFileManagerServer.startAllManager();
    }

    private static void terminate() {
        BlockManagerServer.stopAllManager();
        AlphaFileManagerServer.stopAllManager();
    }

    private static void createFile(String[] list) {
        if (list.length != 3) {
            printHelpHint();
            return;
        }
        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(list[1]);
        FieldId fieldId = new FieldId(list[2]);

        try {
            IFileManager fileManager = AlphaFileManagerServer.getServer(fileManagerId);
            fileManager.newFile(fieldId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void catFile(String[] list) {
        if (list.length != 3) {
            printHelpHint();
            return;
        }
        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(list[1]);
        FieldId fieldId = new FieldId(list[2]);

        try {
            IFileManager fileManager = AlphaFileManagerServer.getServer(fileManagerId);
            IFile file = fileManager.getFile(fieldId);
            file.move(0, file.MOVE_HEAD);
            byte[] data = file.read((int)file.size());
            System.out.println(new String(data));
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void writeFile(String[] list) {
        if (list.length != 5) {
            printHelpHint();
            return;
        }

        AlphaFileManagerId fileManagerId = new AlphaFileManagerId(list[1]);
        FieldId fieldId = new FieldId(list[2]);
        long offset = Long.parseLong(list[3]);
        int where = Integer.parseInt(list[4]);

        try {
            IFileManager fileManager = AlphaFileManagerServer.getServer(fileManagerId);
            IFile file = fileManager.getFile(fieldId);
            file.move(offset, where);
            System.out.println("Please input data:");
            Scanner scanner = new Scanner(System.in);
            String data = scanner.nextLine();
            file.write(data.getBytes());
            file.close();
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void copyFile(String[] list) {
        if (list.length != 5) {
            printHelpHint();
            return;
        }

        AlphaFileManagerId srcFileManagerId = new AlphaFileManagerId(list[1]);
        FieldId srcFieldId = new FieldId(list[2]);
        AlphaFileManagerId dstFileManagerId = new AlphaFileManagerId(list[3]);
        FieldId dstFieldId = new FieldId(list[4]);

        try {
            IFileManager srcFileManager = AlphaFileManagerServer.getServer(srcFileManagerId);
            IFile srcFile = srcFileManager.getFile(srcFieldId);

            IFileManager dstFileManager = AlphaFileManagerServer.getServer(dstFileManagerId);
            IFile dstFile = dstFileManager.getFile(dstFieldId);

            srcFile.copyTo(dstFile);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void hexBlock(String[] list) {
        if (list.length != 3) {
            printHelpHint();
            return;
        }

        BlockManagerId blockManagerId = new BlockManagerId(list[1]);
        BlockId blockId = new BlockId(Long.parseLong(list[2]));

        try {
            BlockManagerServer blockManagerServer = BlockManagerServer.getServer(blockManagerId);
            Block block = blockManagerServer.getBlock(blockId);
            byte[] data = block.read();
            System.out.println(ByteUtils.bytesToHexStr(data));
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void createRemoteFile(String[] list) {
        if (list.length != 5) {
            printHelpHint();
            return;
        }

        String hostName = list[1];
        int port = Integer.parseInt(list[2]);
        String fileManagerIdStr = list[3];
        FieldId fieldId = new FieldId(list[4]);

        try {
            AlphaFileManagerRMIId fileManagerClientId = new AlphaFileManagerRMIId(hostName, port, fileManagerIdStr);
            IFileManager fileManager = AlphaFileManagerClient.getClient(fileManagerClientId);
            fileManager.newFile(fieldId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void catRemoteFile(String[] list) {
        if (list.length != 5) {
            printHelpHint();
            return;
        }

        String hostName = list[1];
        int port = Integer.parseInt(list[2]);
        String fileManagerIdStr = list[3];
        FieldId fieldId = new FieldId(list[4]);

        try {
            AlphaFileManagerRMIId fileManagerClientId = new AlphaFileManagerRMIId(hostName, port, fileManagerIdStr);
            IFileManager fileManager = AlphaFileManagerClient.getClient(fileManagerClientId);
            IFile file = fileManager.getFile(fieldId);
            file.move(0, file.MOVE_HEAD);
            byte[] data = file.read((int)file.size());
            System.out.println(new String(data));
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void writeRemoteFile(String[] list) {
        if (list.length != 7) {
            printHelpHint();
            return;
        }

        String hostName = list[1];
        int port = Integer.parseInt(list[2]);
        String fileManagerIdStr = list[3];
        FieldId fieldId = new FieldId(list[4]);
        long offset = Long.parseLong(list[5]);
        int where = Integer.parseInt(list[6]);

        try {
            AlphaFileManagerRMIId fileManagerClientId = new AlphaFileManagerRMIId(hostName, port, fileManagerIdStr);
            IFileManager fileManager = AlphaFileManagerClient.getClient(fileManagerClientId);
            IFile file = fileManager.getFile(fieldId);
            file.move(offset, where);
            System.out.println("Please input data:");
            Scanner scanner = new Scanner(System.in);
            String data = scanner.nextLine();
            file.write(data.getBytes());
            file.close();
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void copyRemoteFile(String[] list) {
        if (list.length != 9) {
            printHelpHint();
            return;
        }

        String srcHostName = list[1];
        int srcPort = Integer.parseInt(list[2]);
        String srcFileManagerIdStr = list[3];
        FieldId srcFieldId = new FieldId(list[4]);
        String dstHostName = list[5];
        int dstPort = Integer.parseInt(list[6]);
        String dstFileManagerIdStr = list[7];
        FieldId dstFieldId = new FieldId(list[8]);

        try {
            AlphaFileManagerRMIId srcFileManagerClientId = new AlphaFileManagerRMIId(srcHostName, srcPort, srcFileManagerIdStr);
            IFileManager srcFileManager = AlphaFileManagerClient.getClient(srcFileManagerClientId);
            IFile srcFile = srcFileManager.getFile(srcFieldId);

            AlphaFileManagerRMIId dstFileManagerClientId = new AlphaFileManagerRMIId(dstHostName, dstPort, dstFileManagerIdStr);
            IFileManager dstFileManager = AlphaFileManagerClient.getClient(dstFileManagerClientId);
            IFile dstFile = dstFileManager.getFile(dstFieldId);

            srcFile.copyTo(dstFile);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void hexRemoteBlock(String[] list) {
        if (list.length != 5) {
            printHelpHint();
            return;
        }

        String hostName = list[1];
        int port = Integer.parseInt(list[2]);
        String blockManagerIdStr = list[3];
        long blockIdNum = Long.parseLong(list[4]);

        try {
            BlockManagerRMIId blockManagerRMIId = new BlockManagerRMIId(hostName, port, blockManagerIdStr);
            BlockManagerClient blockManagerClient = BlockManagerClient.getClient(blockManagerRMIId);

            Block block = blockManagerClient.getBlock(new BlockClientId(blockIdNum));
            byte[] data = block.read();
            System.out.println(ByteUtils.bytesToHexStr(data));
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void startBlockServer(String[] list) {
        if (list.length != 2) {
            printHelpHint();
            return;
        }

        try {
            BlockManagerId blockManagerId = new BlockManagerId(list[1]);
            BlockManagerServer.startManager(blockManagerId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void stopBlockServer(String[] list) {
        if (list.length != 2) {
            printHelpHint();
            return;
        }

        try {
            BlockManagerId blockManagerId = new BlockManagerId(list[1]);
            BlockManagerServer.stopManager(blockManagerId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void startFileServer(String[] list) {
        if (list.length != 2) {
            printHelpHint();
            return;
        }

        try {
            AlphaFileManagerId fileManagerId = new AlphaFileManagerId(list[1]);
            AlphaFileManagerServer.startManager(fileManagerId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void stopFileServer(String[] list) {
        if (list.length != 2) {
            printHelpHint();
            return;
        }

        try {
            AlphaFileManagerId fileManagerId = new AlphaFileManagerId(list[1]);
            AlphaFileManagerServer.stopManager(fileManagerId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void addBlockClient(String[] list) {
        if (list.length != 4) {
            printHelpHint();
            return;
        }

        try {
            BlockManagerRMIId blockManagerRMIId = new BlockManagerRMIId(list[1], Integer.parseInt(list[2]), list[3]);
            BlockManagerClient.addClient(blockManagerRMIId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void removeBlockClient(String[] list) {
        if (list.length != 4) {
            printHelpHint();
            return;
        }

        try {
            BlockManagerRMIId blockManagerRMIId = new BlockManagerRMIId(list[1], Integer.parseInt(list[2]), list[3]);
            BlockManagerClient.removeClient(blockManagerRMIId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void addFileClient(String[] list) {
        if (list.length != 4) {
            printHelpHint();
            return;
        }

        try {
            AlphaFileManagerRMIId fileManagerRMIId = new AlphaFileManagerRMIId(list[1], Integer.parseInt(list[2]), list[3]);
            AlphaFileManagerClient.addClient(fileManagerRMIId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void removeFileClient(String[] list) {
        if (list.length != 4) {
            printHelpHint();
            return;
        }

        try {
            AlphaFileManagerRMIId fileManagerRMIId = new AlphaFileManagerRMIId(list[1], Integer.parseInt(list[2]), list[3]);
            AlphaFileManagerClient.removeClient(fileManagerRMIId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void printState() {
        BlockManagerServer.listServers();
        BlockManagerClient.listServers();
        AlphaFileManagerServer.listServers();
        AlphaFileManagerClient.listServers();
    }

    private static void printHelpHint() {
        System.out.println("Please input 'help' for command formats.\n");
    }

    private static void printHelp() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Instruction formats:\n");
        stringBuilder.append("create file:    alpha-create  [file manager]  [field]\n");
        stringBuilder.append("print  file:    alpha-cat     [file manager]  [field]\n");
        stringBuilder.append("write  file:    alpha-write   [file manager]  [field]  [offset]  [where]\n");
        stringBuilder.append("copy   file:    alpha-copy    [src file manager]  [src field]  [dst file manager]  [dst field]\n");
        stringBuilder.append("read   block:   alpha-hex     [block manager] [block]\n\n");

        stringBuilder.append("create remote file:    alpha-remote-create  [host name]  [file manager]  [field]\n");
        stringBuilder.append("print  remote file:    alpha-remote-cat     [host name]  [file manager]  [field]\n");
        stringBuilder.append("write  remote file:    alpha-remote-write   [host name]  [file manager]  [field]  [offset]  [where]\n");
        stringBuilder.append("copy   remote file:    alpha-remote-copy    [host name]  [src file manager]  [src field]  [host name]  [dst file manager]  [dst field]\n");
        stringBuilder.append("read   remote block:   alpha-remote-hex     [host name]  [block manager] [block]\n\n");

        stringBuilder.append("start  block manager server:   alpha-start-bm-server  [block manager]\n");
        stringBuilder.append("stop   block manager server:   alpha-stop-bm-server   [block manager]\n");
        stringBuilder.append("start  file  manager server:   alpha-start-fm-server  [file manager]\n");
        stringBuilder.append("stop   file  manager server:   alpha-stop-fm-server   [file manager]\n");
        stringBuilder.append("add    block manager client:   alpha-add-bm-client    [host name]  [port]  [block manager]\n");
        stringBuilder.append("remove block manager client:   alpha-remove-bm-client [host name]  [port]  [block manager]\n");
        stringBuilder.append("add    file  manager client:   alpha-add-fm-client    [host name]  [port]  [file manager]\n");
        stringBuilder.append("remove file  manager client:   alpha-remove-fm-client [host name]  [port]  [file manager]\n\n");

        stringBuilder.append("[where] arg:  CURR : 0   HEAD : 1   TAIL : 2\n\n");

        stringBuilder.append("print state info:  state\n");
        stringBuilder.append("print help  info:  help\n");

        System.out.print(stringBuilder.toString());
    }

    private static void checkConfig() {
        if (ConfigConstants.BLOCK_MANAGER_NUM < ConfigConstants.DUPLICATION_NUM) {
            throw new ErrorCode(ErrorCode.CONFIG_DUPLICATION_MORE_THAN_BLOCK_MANAGER);
        }
    }

    public static void main(String[] args) {
        try {
            Scanner sc = new Scanner(System.in);
            System.out.println("Please input command:");
            while (sc.hasNext()) {
                String command = sc.nextLine();
                String[] list = command.split(" ");
                switch (list[0]) {
                    case "alpha-create":
                        createFile(list);
                        break;
                    case "alpha-cat":
                        catFile(list);
                        break;
                    case "alpha-write":
                        writeFile(list);
                        break;
                    case "alpha-copy":
                        copyFile(list);
                        break;
                    case "alpha-hex":
                        hexBlock(list);
                        break;
                    case "alpha-remote-create":
                        createRemoteFile(list);
                        break;
                    case "alpha-remote-cat":
                        catRemoteFile(list);
                        break;
                    case "alpha-remote-write":
                        writeRemoteFile(list);
                        break;
                    case "alpha-remote-copy":
                        copyRemoteFile(list);
                        break;
                    case "alpha-remote-hex":
                        hexRemoteBlock(list);
                        break;
                    case "alpha-start-bm-server":
                        startBlockServer(list);
                        break;
                    case "alpha-stop-bm-server":
                        stopBlockServer(list);
                        break;
                    case "alpha-start-fm-server":
                        startFileServer(list);
                        break;
                    case "alpha-stop-fm-server":
                        stopFileServer(list);
                        break;
                    case "alpha-add-bm-client":
                        addBlockClient(list);
                        break;
                    case "alpha-remove-bm-client":
                        removeBlockClient(list);
                        break;
                    case "alpha-add-fm-client":
                        addFileClient(list);
                        break;
                    case "alpha-remove-fm-client":
                        removeFileClient(list);
                        break;
                    case "state":
                        printState();
                        break;
                    case "help":
                        printHelp();
                        break;
                    case "quit":
                        terminate();
                        return;
                    default:
                        printHelpHint();
                }
                System.out.println("Please input command:");
            }
        } catch (Exception e) {
            e.printStackTrace();
            ErrorCode error = new ErrorCode(ErrorCode.UNKNOWN);
            System.out.print(error.getMessage());
        }
    }
}
