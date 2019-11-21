package util;

import java.util.HashMap;
import java.util.Map;

public class ErrorCode extends RuntimeException {
    public static final int IO_EXCEPTION = 1;

    // Block Id
    public static final int UNKNOWN_BLOCK_ID = 2;

    // Block
    public static final int NULL_BLOCK_ARGUMENT = 3;
    public static final int INVALID_BLOCK_ID = 4;
    public static final int INVALID_BLOCK_DATA = 5;
    public static final int MD5_INVALID = 6;
    public static final int CHECKSUM_CHECK_FAILED = 7;
    public static final int BLOCK_META_FILE_INVALID = 8;
    public static final int BLOCK_DATA_FILE_INVALID = 9;

    // Block Manager Id
    public static final int UNKNOWN_BLOCK_MANAGER_ID = 10;

    // Block Manager
    public static final int NULL_BLOCK_MANAGER_ARGUMENT = 11;
    public static final int INVALID_BLOCK_MANAGER_ID = 12;
    public static final int NULL_NEW_BLOCK_DATA = 13;
    public static final int BLOCK_MANAGER_META_FILE_INVALID = 14;
    public static final int NULL_BLOCK_ID_ARG = 15;
    public static final int INVALID_BLOCK_ID_ARG = 16;
    public static final int UNKNOWN_BLOCK_INDEX_ID = 17;

    // File Id
    public static final int UNKNOWN_FILE_ID = 18;

    // File
    public static final int NULL_FILE_ARGUMENT = 19;
    public static final int INVALID_FILE_ID = 20;
    public static final int INVALID_READ_LENGTH = 21;
    public static final int UNAVAILABLE_LOGIC_BLOCK = 22;
    public static final int INVALID_WHERE_ARG = 23;
    public static final int INVALID_POINTER_POS_UNDERFLOW = 24;
    public static final int FILE_META_FILE_INVALID = 25;
    public static final int NULL_FILE_WRITE_IN_DATA = 26;
    public static final int NEGATIVE_FILE_NEW_SIZE = 27;
    public static final int NULL_COPY_DST_FILE = 28;
    public static final int INVALID_COPY_DST_FILE = 29;

    // File Manager Id
    public static final int UNKNOWN_FILE_MANAGER_ID = 30;

    // File Manager
    public static final int NULL_FILE_MANAGER_ARGUMENT = 31;
    public static final int INVALID_FILE_MANAGER_ID = 32;
    public static final int NULL_FIELD_ID = 33;
    public static final int FILE_MANAGER_META_FILE_INVALID = 34;
    public static final int INVALID_FIELD_ID_ARG = 35;
    public static final int UNKNOWN_FIELD_ID = 36;

    // Config
    public static final int CONFIG_DUPLICATION_MORE_THAN_BLOCK_MANAGER = 37;

    // Block Manager RMI
    public static final int NULL_BLOCK_RMI_ID_ARG = 38;
    public static final int NULL_BLOCK_RMI_DATA_ARG = 39;
    public static final int TERMINATE_UNKNOWN_BLOCK_RMI = 40;

    // Block Manager Server
    public static final int NULL_BLOCK_MANAGER_SERVER_ID = 41;
    public static final int EXISTING_BLOCK_MANAGER_SERVER_ID = 42;
    public static final int UNKNOWN_BLOCK_MANAGER_SERVER_ID = 43;
    public static final int BLOCK_MANAGER_NOT_SERVING = 44;
    public static final int BLOCK_MANAGER_NO_SERVING = 45;
    public static final int BLOCK_MANAGER_SERVER_LAUNCH_FAILURE = 46;
    public static final int ALREADY_BOUND_BLOCK_RMI = 47;
    public static final int LACKING_SERVER_FOR_DUPLICATION = 48;
    public static final int UNKNOWN_BLOCK_MANAGER_SERVER_EXCEPTION = 49;

    // Block Manager Client Id
    public static final int NULL_BLOCK_MANAGER_CLIENT_ID_ARG = 50;

    // Block Client Id
    public static final int BLOCK_CLIENT_INVALID_BLOCK_ID = 51;

    // Block Manager Client
    public static final int BLOCK_MANAGER_CLIENT_CONNECT_FAILURE = 52;
    public static final int NOT_BOUND_BLOCK_RMI = 53;
    public static final int BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION = 54;
    public static final int FAILED_TO_SET_SOCKET_TIMEOUT = 55;
    public static final int NULL_BLOCK_MANAGER_CLIENT_ID = 56;
    public static final int EXISTING_BLOCK_MANAGER_CLIENT_ID = 57;
    public static final int UNKNOWN_BLOCK_MANAGER_CLIENT_ID = 58;
    public static final int BLOCK_CLIENT_INVALID_BLOCK_TYPE = 59;

    // File Manager Server
    public static final int NULL_FILE_MANAGER_CONTROLLER_ID = 60;
    public static final int UNKNOWN_FILE_MANAGER_CONTROLLER_ID = 61;
    public static final int EXISTING_FILE_MANAGER_CONTROLLER_ID = 62;
    public static final int FILE_MANAGER_NOT_SERVING = 63;
    public static final int FILE_MANAGER_NO_SERVING = 64;

    // UNKNOWN
    public static final int UNKNOWN = 1000;

    private static final Map<Integer, String> ErrorCodeMap = new HashMap<>();

    static {
        ErrorCodeMap.put(IO_EXCEPTION, "IO exception: ");

        // Block Id
        ErrorCodeMap.put(UNKNOWN_BLOCK_ID, "Unknown block id: ");

        // Block
        ErrorCodeMap.put(NULL_BLOCK_ARGUMENT, "Null block argument");
        ErrorCodeMap.put(INVALID_BLOCK_ID, "Invalid block id in blockId.count");
        ErrorCodeMap.put(INVALID_BLOCK_DATA, "Data array size larger than block size");
        ErrorCodeMap.put(MD5_INVALID, "MD5 invalid");
        ErrorCodeMap.put(CHECKSUM_CHECK_FAILED, "Block read checksum failed");
        ErrorCodeMap.put(BLOCK_META_FILE_INVALID, "Invalid block meta file");
        ErrorCodeMap.put(BLOCK_DATA_FILE_INVALID, "Invalid block data file");

        // Block Manager Id
        ErrorCodeMap.put(UNKNOWN_BLOCK_MANAGER_ID, "Unknown block manager id: ");

        // Block Manager
        ErrorCodeMap.put(NULL_BLOCK_MANAGER_ARGUMENT, "Null block manager argument");
        ErrorCodeMap.put(INVALID_BLOCK_MANAGER_ID, "Invalid block manager id in blockManagerId.count");
        ErrorCodeMap.put(NULL_NEW_BLOCK_DATA, "Null new block data byte argument");
        ErrorCodeMap.put(BLOCK_MANAGER_META_FILE_INVALID, "Invalid block manager meta file");
        ErrorCodeMap.put(NULL_BLOCK_ID_ARG, "Null block id argument");
        ErrorCodeMap.put(INVALID_BLOCK_ID_ARG, "Invalid block id argument");
        ErrorCodeMap.put(UNKNOWN_BLOCK_INDEX_ID, "Unknown block index id: ");

        // File Id
        ErrorCodeMap.put(UNKNOWN_FILE_ID, "Unknown file id: ");

        // File
        ErrorCodeMap.put(NULL_FILE_ARGUMENT, "Null file argument");
        ErrorCodeMap.put(INVALID_FILE_ID, "Invalid file id in fileId.count");
        ErrorCodeMap.put(INVALID_READ_LENGTH, "Invalid read length");
        ErrorCodeMap.put(UNAVAILABLE_LOGIC_BLOCK, "Unavailable logic block");
        ErrorCodeMap.put(INVALID_WHERE_ARG, "Invalid where arg");
        ErrorCodeMap.put(INVALID_POINTER_POS_UNDERFLOW, "Invalid new underflow pointer position");
        ErrorCodeMap.put(FILE_META_FILE_INVALID, "Invalid file meta file");
        ErrorCodeMap.put(NULL_FILE_WRITE_IN_DATA, "Null write-in data");
        ErrorCodeMap.put(NEGATIVE_FILE_NEW_SIZE, "File size should not be negative");
        ErrorCodeMap.put(NULL_COPY_DST_FILE, "Null copy destination file");
        ErrorCodeMap.put(INVALID_COPY_DST_FILE, "Invalid copy destination file type");

        // File Manager Id
        ErrorCodeMap.put(UNKNOWN_FILE_MANAGER_ID, "Unknown file manager id: ");

        // File Manager
        ErrorCodeMap.put(NULL_FILE_MANAGER_ARGUMENT, "Null file manager argument");
        ErrorCodeMap.put(INVALID_FILE_MANAGER_ID, "Invalid file manager id in fileManagerId.count");
        ErrorCodeMap.put(NULL_FIELD_ID, "Null field id argument");
        ErrorCodeMap.put(FILE_MANAGER_META_FILE_INVALID, "Invalid file manager meta file");
        ErrorCodeMap.put(INVALID_FIELD_ID_ARG, "Invalid field id argument");
        ErrorCodeMap.put(UNKNOWN_FIELD_ID, "Unknown field id: ");

        // Config
        ErrorCodeMap.put(CONFIG_DUPLICATION_MORE_THAN_BLOCK_MANAGER, "Duplication should be less than block managers");

        // Block Manager RMI
        ErrorCodeMap.put(NULL_BLOCK_RMI_ID_ARG, "Null id argument to block manager RMI");
        ErrorCodeMap.put(NULL_BLOCK_RMI_DATA_ARG, "Null data argument to block manager RMI");
        ErrorCodeMap.put(TERMINATE_UNKNOWN_BLOCK_RMI, "Try to terminate unknown block manager RMI service");

        // Block Manager Server
        ErrorCodeMap.put(NULL_BLOCK_MANAGER_SERVER_ID, "Null block manager id argument to controller");
        ErrorCodeMap.put(EXISTING_BLOCK_MANAGER_SERVER_ID, "Add existing block manager id to controller: ");
        ErrorCodeMap.put(UNKNOWN_BLOCK_MANAGER_SERVER_ID, "Unknown block manager id argument to controller: ");
        ErrorCodeMap.put(BLOCK_MANAGER_NOT_SERVING, "Block manager is not serving: ");
        ErrorCodeMap.put(BLOCK_MANAGER_NO_SERVING, "No block manager is serving");
        ErrorCodeMap.put(BLOCK_MANAGER_SERVER_LAUNCH_FAILURE, "Failed to launch server");
        ErrorCodeMap.put(ALREADY_BOUND_BLOCK_RMI, "Block manager RMI already bounded");
        ErrorCodeMap.put(LACKING_SERVER_FOR_DUPLICATION, "Running servers can not meet duplication config requirement");
        ErrorCodeMap.put(UNKNOWN_BLOCK_MANAGER_SERVER_EXCEPTION, "Unknown block manager server exception, server stop running: ");

        // Block Manager Client Id
        ErrorCodeMap.put(NULL_BLOCK_MANAGER_CLIENT_ID_ARG, "Null argument passed to block manager client id");

        // Block Client Id
        ErrorCodeMap.put(BLOCK_CLIENT_INVALID_BLOCK_ID, "Block id passed to block manager client should not be non-positive");

        // Block Manager Client
        ErrorCodeMap.put(BLOCK_MANAGER_CLIENT_CONNECT_FAILURE, "Failed to connect client: ");
        ErrorCodeMap.put(NOT_BOUND_BLOCK_RMI, "Block manager RMI not bounded: ");
        ErrorCodeMap.put(BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION, "Block manager client remote exception:\n");
        ErrorCodeMap.put(FAILED_TO_SET_SOCKET_TIMEOUT, "Failed to set socket timeout");
        ErrorCodeMap.put(NULL_BLOCK_MANAGER_CLIENT_ID, "Null block manager client id");
        ErrorCodeMap.put(EXISTING_BLOCK_MANAGER_CLIENT_ID, "Add existing block manager client id to controller: ");
        ErrorCodeMap.put(UNKNOWN_BLOCK_MANAGER_CLIENT_ID, "Unknown block manager client id argument to controller: ");
        ErrorCodeMap.put(BLOCK_CLIENT_INVALID_BLOCK_TYPE, "Block id type passed to block manager client invalid");

        // File Manager Server
        ErrorCodeMap.put(NULL_FILE_MANAGER_CONTROLLER_ID, "Null file manager id argument to controller");
        ErrorCodeMap.put(UNKNOWN_FILE_MANAGER_CONTROLLER_ID, "Unknown file manager id argument to controller: ");
        ErrorCodeMap.put(EXISTING_FILE_MANAGER_CONTROLLER_ID, "Add existing file manager id to controller");
        ErrorCodeMap.put(FILE_MANAGER_NOT_SERVING, "File manager is not serving: ");
        ErrorCodeMap.put(FILE_MANAGER_NO_SERVING, "No file manager is serving");

        // Unknown
        ErrorCodeMap.put(UNKNOWN, "Unknown exception");
    }

    public static String getErrorText(int errorCode) {
        return ErrorCodeMap.getOrDefault(errorCode, "invalid");
    }

    private int errorCode;

    public ErrorCode(int errorCode) {
        super(String.format("Error code '%d' \"%s\"", errorCode, getErrorText(errorCode)));
        this.errorCode = errorCode;
    }

    public ErrorCode(int errorCode, String arg) {
        super(String.format("Error code '%d' \"%s\"", errorCode, getErrorText(errorCode) + arg));
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
