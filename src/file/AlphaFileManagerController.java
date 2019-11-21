package file;

import constant.PathConstants;
import util.ByteUtils;
import util.ErrorCode;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;

public class AlphaFileManagerController {
    private static HashMap<AlphaFileManagerId, Boolean> controller;

    public static void init() {
        controller = new HashMap<>();

        File file = new File(PathConstants.FILE_MANAGER_PATH, PathConstants.FILE_MANAGER_ID_COUNT);
        long blockManagerNum;

        try {
            BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(file));

            // read long-type block manager id count
            byte[] bytes = new byte[Long.BYTES];
            if (inputStream.read(bytes) != bytes.length)
                throw new ErrorCode(ErrorCode.INVALID_FILE_MANAGER_ID);
            inputStream.close();

            blockManagerNum = ByteUtils.bytesToLong(bytes);
        } catch (IOException e) {
            throw new ErrorCode(ErrorCode.IO_EXCEPTION, file.getPath());
        }

        for (int i = 1; i < blockManagerNum; ++i) {
            controller.put(new AlphaFileManagerId(PathConstants.FILE_MANAGER_PREFIX + i), false);
        }
    }

    public static void startManager(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CONTROLLER_ID);
        }
        if (null == controller.replace(fileManagerId, true)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_CONTROLLER_ID, fileManagerId.getId());
        }
    }

    public static void stopManager(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CONTROLLER_ID);
        }
        if (null == controller.replace(fileManagerId, false)) {
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_CONTROLLER_ID, fileManagerId.getId());
        }
    }

    public static void startAllManager() {
        for (AlphaFileManagerId fileManagerId : controller.keySet()) {
            startManager(fileManagerId);
        }
    }

    public static void stopAllManager() {
        for (AlphaFileManagerId fileManagerId : controller.keySet()) {
            stopManager(fileManagerId);
        }
    }

    public static boolean isServing(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CONTROLLER_ID);
        }

        if (controller.containsKey(fileManagerId)) {
            return controller.get(fileManagerId);
        } else {
            throw new ErrorCode(ErrorCode.UNKNOWN_FILE_MANAGER_CONTROLLER_ID, fileManagerId.getId());
        }
    }

    public static boolean noServing() {
        for (AlphaFileManagerId fileManagerId : controller.keySet()) {
            if (controller.get(fileManagerId))
                return false;   // exist serving manager
        }
        return true;    // No serving manager
    }

    public static void addManager(AlphaFileManagerId fileManagerId) {
        if (null == fileManagerId) {
            throw new ErrorCode(ErrorCode.NULL_FILE_MANAGER_CONTROLLER_ID);
        }

        if (!controller.containsKey(fileManagerId)) {
            throw new ErrorCode(ErrorCode.EXISTING_FILE_MANAGER_CONTROLLER_ID, fileManagerId.getId());
        } else {
            controller.put(fileManagerId, true);
        }
    }
}
