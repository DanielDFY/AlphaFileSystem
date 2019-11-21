import block.Block;
import block.BlockClientId;
import block.BlockId;
import block.BlockManagerClient;
import block.BlockManagerClientId;
import block.BlockManagerId;
import block.BlockManagerServer;
import constant.ConfigConstants;
import constant.PathConstants;
import file.AlphaFileManager;
import file.AlphaFileManagerId;
import file.FieldId;
import file.IFile;
import util.ByteUtils;
import util.ErrorCode;

import java.io.File;
import java.util.Scanner;

public class AlphaFileSystem {
    private static void init() {
        File systemDir = new File(PathConstants.SystemPath);
        if (!systemDir.exists()) {
            if (!systemDir.mkdir()) {
                throw new ErrorCode(ErrorCode.IO_EXCEPTION, systemDir.getPath());
            }
        }

        /*
        File fileManagerDir = new File(PathConstants.FILE_MANAGER_PATH);
        if (!fileManagerDir.mkdir()) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, fileManagerDir.getPath());
        }

        File fileCount = new File(PathConstants.FILE_MANAGER_PATH, PathConstants.FILE_ID_COUNT);
        try {
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(fileCount));
            outputStream.write(ByteUtils.longToBytes(0));
            outputStream.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        File fileManagerCount = new File(PathConstants.FILE_MANAGER_PATH, PathConstants.FILE_MANAGER_ID_COUNT);
        try {
            BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(fileManagerCount));
            outputStream.write(ByteUtils.longToBytes(0));
            outputStream.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

         */

        // todo
        // init server
        BlockManagerServer.init();

        /*
        for (int i = 0; i < ConfigConstants.FILE_MANAGER_NUM; ++i) {
            new AlphaFileManager();
        }

         */
    }

    private static void terminate() {
        BlockManagerServer.terminateRMI();
    }

    private static void catFile(AlphaFileManagerId fileManagerId, FieldId fieldId) {
        try {
            AlphaFileManager fileManager = new AlphaFileManager(fileManagerId);
            IFile file = fileManager.getFile(fieldId);
            byte[] data = file.read((int)file.size());
            System.out.println(new String(data));
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void hexBlock(BlockManagerId blockManagerId, BlockId blockId) {
        try {
            Block block = new Block(blockManagerId, blockId);
            byte[] data = block.read();
            System.out.println(ByteUtils.bytesToHexStr(data));
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void writeFile(AlphaFileManagerId fileManagerId, FieldId fieldId, long offset, int where) {
        try {
            AlphaFileManager fileManager = new AlphaFileManager(fileManagerId);
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
            AlphaFileManager srcFileManager = new AlphaFileManager(srcFileManagerId);
            IFile srcFile = srcFileManager.getFile(srcFieldId);

            AlphaFileManager dstFileManager = new AlphaFileManager(dstFileManagerId);
            IFile dstFile = dstFileManager.getFile(dstFieldId);

            srcFile.copyTo(dstFile);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void createFile(AlphaFileManagerId fileManagerId, FieldId fieldId) {
        try {
            AlphaFileManager fileManager = new AlphaFileManager(fileManagerId);
            fileManager.newFile(fieldId);
        } catch (ErrorCode e) {
            System.out.println(e.getMessage());
        }
    }

    private static void printHelp() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Instruction formats:\n");
        stringBuilder.append("create file:    alpha-create  [file manager]  [field]\n");
        stringBuilder.append("print  file:    alpha-cat     [file manager]  [field]\n");
        stringBuilder.append("write  file:    alpha-write   [file manager]  [field]  [offset]  [where]\n");
        stringBuilder.append("copy   file:    alpha-copy    [src file manager]  [src field]  [dst file manager]  [dst field]\n");
        stringBuilder.append("read   block:   alpha-hex     [block manager] [block]\n");
        stringBuilder.append("[where] arg:  CURR : 0   HEAD : 1   TAIL : 2\n");
        System.out.print(stringBuilder.toString());
    }

    private static void checkConfig() {
        if (ConfigConstants.BLOCK_MANAGER_NUM < ConfigConstants.DUPLICATION_NUM) {
            throw new ErrorCode(ErrorCode.CONFIG_DUPLICATION_MORE_THAN_BLOCK_MANAGER);
        }
    }

    private static void bmServerTest() {
        long start, end;

        BlockManagerId id = new BlockManagerId("bm1");
        BlockManagerClientId blockManagerClientId = new BlockManagerClientId("localhost", id.getId());
        BlockManagerClient blockManagerClient = BlockManagerClient.getClient(blockManagerClientId);

        start = System.currentTimeMillis();
        Block block = blockManagerClient.getBlock(new BlockClientId(1));
        end = System.currentTimeMillis();
        System.out.println("time: " + (end - start) + "ms");
        System.out.println(new String(block.read()));
    }

    public static void main(String[] args) {
        checkConfig();

        try {
            init();

            BlockManagerId id = new BlockManagerId("bm1");
            BlockManagerServer blockManagerServer = BlockManagerServer.getServer(id);
            BlockManagerServer.startManager(id);
            blockManagerServer.newBlock("hello".getBytes());
            BlockManagerClientId blockManagerClientId = new BlockManagerClientId("localhost", id.getId());
            BlockManagerClient.addClient(blockManagerClientId);

            Scanner sc = new Scanner(System.in);
            System.out.println("Please input command:");
            while (sc.hasNext()) {
                String command = sc.nextLine();
                String[] list = command.split(" ");
                switch (list[0]) {
                    case "alpha-create":
                        if (list.length != 3) {
                            System.out.println("Please input 'help' for command formats.\n");
                            break;
                        }
                        createFile(new AlphaFileManagerId(list[1]), new FieldId(list[2]));
                        break;
                    case "alpha-cat":
                        if (list.length != 3) {
                            System.out.println("Please input 'help' for command formats.\n");
                            break;
                        }
                        catFile(new AlphaFileManagerId(list[1]), new FieldId(list[2]));
                        break;
                    case "alpha-write":
                        if (list.length != 5) {
                            System.out.println("Please input 'help' for command formats.\n");
                            break;
                        }
                        writeFile(new AlphaFileManagerId(list[1]), new FieldId(list[2]), Long.parseLong(list[3]), Integer.parseInt(list[4]));
                        break;
                    case "alpha-copy":
                        if (list.length != 5) {
                            System.out.println("Please input 'help' for command formats.\n");
                            break;
                        }
                        copyFile(new AlphaFileManagerId(list[1]), new FieldId(list[2]), new AlphaFileManagerId(list[3]), new FieldId(list[4]));
                        break;
                    case "alpha-hex":
                        if (list.length != 3) {
                            System.out.println("Please input 'help' for command formats.\n");
                            break;
                        }
                        hexBlock(new BlockManagerId(list[1]), new BlockId(Long.parseLong(list[2])));
                        break;
                    case "help":
                        printHelp();
                        break;
                    case "quit":
                        terminate();
                        return;
                    case "bmServer-test":
                        bmServerTest();
                        break;
                    default:
                        System.out.println("Please input 'help' for command formats.\n");
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
