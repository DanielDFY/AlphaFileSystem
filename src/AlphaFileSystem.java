import block.Block;
import block.BlockClientId;
import block.BlockId;
import block.BlockManagerClient;
import block.BlockManagerClientId;
import block.BlockManagerId;
import block.BlockManagerServer;
import constant.ConfigConstants;
import constant.PathConstants;
import file.AlphaFileManagerClient;
import file.AlphaFileManagerClientId;
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

        File systemDir = new File(PathConstants.SystemPath);
        if (!systemDir.exists()) {
            if (!systemDir.mkdir()) {
                throw new ErrorCode(ErrorCode.IO_EXCEPTION, systemDir.getPath());
            }
        }

        // init server
        BlockManagerServer.init();
        AlphaFileManagerServer.init();
    }

    private static void terminate() {
        BlockManagerServer.terminateRMI();
        AlphaFileManagerServer.terminateRMI();
    }

    private static void createFile(AlphaFileManagerId fileManagerId, FieldId fieldId) {
        try {
            IFileManager fileManager = AlphaFileManagerServer.getServer(fileManagerId);
            fileManager.newFile(fieldId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void catFile(AlphaFileManagerId fileManagerId, FieldId fieldId) {
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

    private static void writeFile(AlphaFileManagerId fileManagerId, FieldId fieldId, long offset, int where) {
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

    private static void copyFile(AlphaFileManagerId srcFileManagerId, FieldId srcFieldId, AlphaFileManagerId dstFileManagerId, FieldId dstFieldId) {
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

    private static void hexBlock(BlockManagerId blockManagerId, BlockId blockId) {
        try {
            BlockManagerServer blockManagerServer = BlockManagerServer.getServer(blockManagerId);
            Block block = blockManagerServer.getBlock(blockId);
            byte[] data = block.read();
            System.out.println(ByteUtils.bytesToHexStr(data));
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void createRemoteFile(String hostName, String fileManagerIdStr, FieldId fieldId) {
        try {
            AlphaFileManagerClientId fileManagerClientId = new AlphaFileManagerClientId(hostName, fileManagerIdStr);
            IFileManager fileManager = AlphaFileManagerClient.getClient(fileManagerClientId);
            fileManager.newFile(fieldId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void catRemoteFile(String hostName, String fileManagerIdStr, FieldId fieldId) {
        try {
            AlphaFileManagerClientId fileManagerClientId = new AlphaFileManagerClientId(hostName, fileManagerIdStr);
            IFileManager fileManager = AlphaFileManagerClient.getClient(fileManagerClientId);
            IFile file = fileManager.getFile(fieldId);
            file.move(0, file.MOVE_HEAD);
            byte[] data = file.read((int)file.size());
            System.out.println(new String(data));
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void writeRemoteFile(String hostName, String fileManagerIdStr, FieldId fieldId, long offset, int where) {
        try {
            AlphaFileManagerClientId fileManagerClientId = new AlphaFileManagerClientId(hostName, fileManagerIdStr);
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

    private static void copyRemoteFile(String srcHostName, String srcFileManagerIdStr, FieldId srcFieldId, String dstHostName, String dstFileManagerIdStr, FieldId dstFieldId) {
        try {
            AlphaFileManagerClientId srcFileManagerClientId = new AlphaFileManagerClientId(srcHostName, srcFileManagerIdStr);
            IFileManager srcFileManager = AlphaFileManagerClient.getClient(srcFileManagerClientId);
            IFile srcFile = srcFileManager.getFile(srcFieldId);

            AlphaFileManagerClientId dstFileManagerClientId = new AlphaFileManagerClientId(dstHostName, dstFileManagerIdStr);
            IFileManager dstFileManager = AlphaFileManagerClient.getClient(dstFileManagerClientId);
            IFile dstFile = dstFileManager.getFile(dstFieldId);

            srcFile.copyTo(dstFile);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void hexRemoteBlock(String hostName, String blockManagerIdStr, long blockIdNum) {
        try {
            BlockManagerClientId blockManagerClientId = new BlockManagerClientId(hostName, blockManagerIdStr);
            BlockManagerClient blockManagerClient = BlockManagerClient.getClient(blockManagerClientId);

            Block block = blockManagerClient.getBlock(new BlockClientId(blockIdNum));
            byte[] data = block.read();
            System.out.println(ByteUtils.bytesToHexStr(data));
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
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
        stringBuilder.append("read   block:   alpha-hex     [block manager] [block]\n");
        stringBuilder.append("create remote file:    alpha-remote-create  [host name]  [file manager]  [field]\n");
        stringBuilder.append("print  remote file:    alpha-remote-cat     [host name]  [file manager]  [field]\n");
        stringBuilder.append("write  remote file:    alpha-remote-write   [host name]  [file manager]  [field]  [offset]  [where]\n");
        stringBuilder.append("copy   remote file:    alpha-remote-copy    [host name]  [src file manager]  [src field]  [host name]  [dst file manager]  [dst field]\n");
        stringBuilder.append("read   remote block:   alpha-remote-hex     [host name]  [block manager] [block]\n");
        stringBuilder.append("[where] arg:  CURR : 0   HEAD : 1   TAIL : 2\n");
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
                        if (list.length != 3) {
                            printHelpHint();
                            break;
                        }
                        createFile(new AlphaFileManagerId(list[1]), new FieldId(list[2]));
                        break;
                    case "alpha-cat":
                        if (list.length != 3) {
                            printHelpHint();
                            break;
                        }
                        catFile(new AlphaFileManagerId(list[1]), new FieldId(list[2]));
                        break;
                    case "alpha-write":
                        if (list.length != 5) {
                            printHelpHint();
                            break;
                        }
                        writeFile(new AlphaFileManagerId(list[1]), new FieldId(list[2]), Long.parseLong(list[3]), Integer.parseInt(list[4]));
                        break;
                    case "alpha-copy":
                        if (list.length != 5) {
                            printHelpHint();
                            break;
                        }
                        copyFile(new AlphaFileManagerId(list[1]), new FieldId(list[2]), new AlphaFileManagerId(list[3]), new FieldId(list[4]));
                        break;
                    case "alpha-hex":
                        if (list.length != 3) {
                            printHelpHint();
                            break;
                        }
                        hexBlock(new BlockManagerId(list[1]), new BlockId(Long.parseLong(list[2])));
                        break;
                    case "alpha-remote-create":
                        if (list.length != 4) {
                            printHelpHint();
                            break;
                        }
                        createRemoteFile(list[1], list[2], new FieldId(list[3]));
                        break;
                    case "alpha-remote-cat":
                        if (list.length != 4) {
                            printHelpHint();
                            break;
                        }
                        catRemoteFile(list[1], list[2], new FieldId(list[3]));
                        break;
                    case "alpha-remote-write":
                        if (list.length != 6) {
                            printHelpHint();
                            break;
                        }
                        writeRemoteFile(list[1], list[2], new FieldId(list[3]), Long.parseLong(list[4]), Integer.parseInt(list[5]));
                        break;
                    case "alpha-remote-copy":
                        if (list.length != 7) {
                            printHelpHint();
                            break;
                        }
                        copyRemoteFile(list[1], list[2], new FieldId(list[3]), list[4], list[5], new FieldId(list[6]));
                        break;
                    case "alpha-remote-hex":
                        if (list.length != 4) {
                            printHelpHint();
                            break;
                        }
                        hexRemoteBlock(list[1], list[2], Long.parseLong(list[3]));
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
