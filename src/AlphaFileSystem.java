import block.Block;
import block.BlockId;
import block.BlockManager;
import block.BlockManagerId;
import block.IBlock;
import constant.ConfigConstants;
import constant.PathConstants;
import file.AlphaFileManager;
import file.AlphaFileManagerId;
import file.FieldId;
import file.IFile;
import util.ByteUtils;
import util.ErrorCode;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Scanner;

public class AlphaFileSystem {
    private static void init() {
        File systemDir = new File(PathConstants.SystemPath);
        if (systemDir.exists())
            return;

        if (!systemDir.mkdir()) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        }

        File blockManagerDir = new File(PathConstants.BLOCK_MANAGER_PATH);
        if (!blockManagerDir.mkdir()) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
        }

        File blockCount = new File(PathConstants.BLOCK_MANAGER_PATH, PathConstants.BLOCK_ID_COUNT);
        try {
            BufferedOutputStream inputStream = new BufferedOutputStream(new FileOutputStream(blockCount));
            inputStream.write(ByteUtils.longToBytes(0));
            inputStream.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        File blockManagerCount = new File(PathConstants.BLOCK_MANAGER_PATH, PathConstants.BLOCK_MANAGER_ID_COUNT);
        try {
            BufferedOutputStream inputStream = new BufferedOutputStream(new FileOutputStream(blockManagerCount));
            inputStream.write(ByteUtils.longToBytes(0));
            inputStream.close();
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        File fileManagerDir = new File(PathConstants.FILE_MANAGER_PATH);
        if (!fileManagerDir.mkdir()) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION);
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


        for (int i = 0; i < ConfigConstants.BLOCK_MANAGER_NUM; ++i) {
            new BlockManager();
        }

        for (int i = 0; i < ConfigConstants.FILE_MANAGER_NUM; ++i) {
            new AlphaFileManager();
        }
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
            IBlock block = new Block(blockManagerId, blockId);
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

    public static void main(String[] args) {
        checkConfig();

        try {
            init();
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
                        return;
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
