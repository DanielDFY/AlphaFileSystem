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
    public static final int FAILED_TO_SET_SOCKET_TIMEOUT = 38;

    // Registry
    public static final int MANAGER_REGISTRY_LAUNCH_FAILURE = 39;

    // Block Manager RMI
    public static final int NULL_BLOCK_MANAGER_RMI_ID_ARG = 40;
    public static final int NULL_BLOCK_RMI_DATA_ARG = 41;
    public static final int TERMINATE_UNKNOWN_BLOCK_RMI = 42;
    public static final int TERMINATE_BLOCK_RMI_ERROR = 43;

    // Block Manager Server
    public static final int NULL_BLOCK_MANAGER_SERVER_ID = 44;
    public static final int UNKNOWN_BLOCK_MANAGER_SERVER_ID = 45;
    public static final int EXISTING_BLOCK_MANAGER_SERVER_ID = 46;
    public static final int BLOCK_MANAGER_NOT_SERVING = 47;
    public static final int BLOCK_MANAGER_NO_SERVING = 48;
    public static final int BOUND_BLOCK_RMI_ERROR = 49;
    public static final int LACKING_SERVER_FOR_DUPLICATION = 50;
    public static final int UNKNOWN_BLOCK_MANAGER_SERVER_EXCEPTION = 51;
    public static final int RESTART_BLOCK_MANAGER_SERVER_EXCEPTION = 52;

    // Block Manager Client Id
    public static final int NULL_BLOCK_MANAGER_CLIENT_ID_ARG = 53;

    // Block Client Id
    public static final int BLOCK_CLIENT_INVALID_BLOCK_ID = 54;

    // Block Manager Client
    public static final int BLOCK_MANAGER_CLIENT_CONNECT_FAILURE = 55;
    public static final int NOT_BOUND_BLOCK_RMI = 56;
    public static final int BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION = 57;
    public static final int NULL_BLOCK_MANAGER_CLIENT_ID = 58;
    public static final int EXISTING_BLOCK_MANAGER_CLIENT_ID = 59;
    public static final int UNKNOWN_BLOCK_MANAGER_CLIENT_ID = 60;
    public static final int BLOCK_CLIENT_INVALID_BLOCK_TYPE = 61;

    // File Manager RMI
    public static final int NULL_FILE_RMI_MANAGER_ID_ARG = 62;
    public static final int NULL_FILE_RMI_FIELD_ID_ARG = 63;
    public static final int TERMINATE_UNKNOWN_FILE_RMI = 64;
    public static final int TERMINATE_FILE_RMI_ERROR = 65;
    public static final int NULL_FILE_RMI_FILE_ARG = 66;
    public static final int INVALID_FILE_RMI_FILE_ARG = 67;

    // File Manager Server
    public static final int NULL_FILE_MANAGER_SERVER_ID = 68;
    public static final int UNKNOWN_FILE_MANAGER_SERVER_ID = 69;
    public static final int EXISTING_FILE_MANAGER_SERVER_ID = 70;
    public static final int FILE_MANAGER_NOT_SERVING = 71;
    public static final int FILE_MANAGER_SERVER_LAUNCH_FAILURE = 72;
    public static final int BOUND_FILE_RMI_ERROR = 73;
    public static final int UNKNOWN_FILE_MANAGER_SERVER_EXCEPTION = 74;
    public static final int RESTART_FILE_MANAGER_SERVER_EXCEPTION = 75;

    // File Manager Client Id
    public static final int NULL_FILE_MANAGER_CLIENT_ID_ARG = 76;

    // File Manager Client
    public static final int FILE_MANAGER_CLIENT_CONNECT_FAILURE = 77;
    public static final int NOT_BOUND_FILE_RMI = 78;
    public static final int FILE_MANAGER_CLIENT_REMOTE_EXCEPTION = 79;
    public static final int NULL_FILE_MANAGER_CLIENT_ID = 80;
    public static final int EXISTING_FILE_MANAGER_CLIENT_ID = 81;
    public static final int UNKNOWN_FILE_MANAGER_CLIENT_ID = 82;
    public static final int FILE_CLIENT_INVALID_FIELD_TYPE = 83;

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
        ErrorCodeMap.put(FAILED_TO_SET_SOCKET_TIMEOUT, "Failed to set socket timeout");

        // Registry
        ErrorCodeMap.put(MANAGER_REGISTRY_LAUNCH_FAILURE, "Failed to launch server registry");

        // Block Manager RMI
        ErrorCodeMap.put(NULL_BLOCK_MANAGER_RMI_ID_ARG, "Null id argument to block manager RMI");
        ErrorCodeMap.put(NULL_BLOCK_RMI_DATA_ARG, "Null data argument to block manager RMI");
        ErrorCodeMap.put(TERMINATE_UNKNOWN_BLOCK_RMI, "Try to terminate unknown block manager RMI service");
        ErrorCodeMap.put(TERMINATE_BLOCK_RMI_ERROR, "Error to terminate block manager RMI service: ");

        // Block Manager Server
        ErrorCodeMap.put(NULL_BLOCK_MANAGER_SERVER_ID, "Null block manager id argument to controller");
        ErrorCodeMap.put(UNKNOWN_BLOCK_MANAGER_SERVER_ID, "Unknown block manager id argument to controller: ");
        ErrorCodeMap.put(EXISTING_BLOCK_MANAGER_SERVER_ID, "Add existing block manager id to controller: ");
        ErrorCodeMap.put(BLOCK_MANAGER_NOT_SERVING, "Block manager is not serving: ");
        ErrorCodeMap.put(BLOCK_MANAGER_NO_SERVING, "No block manager is serving");
        ErrorCodeMap.put(BOUND_BLOCK_RMI_ERROR, "Block manager RMI bounded error");
        ErrorCodeMap.put(LACKING_SERVER_FOR_DUPLICATION, "Running servers can not meet duplication config requirement");
        ErrorCodeMap.put(UNKNOWN_BLOCK_MANAGER_SERVER_EXCEPTION, "Unknown block manager server exception, server stop running: ");
        ErrorCodeMap.put(RESTART_BLOCK_MANAGER_SERVER_EXCEPTION, "Unknown block manager server exception, restart server: ");

        // Block Manager Client Id
        ErrorCodeMap.put(NULL_BLOCK_MANAGER_CLIENT_ID_ARG, "Null argument passed to block manager client id");

        // Block Client Id
        ErrorCodeMap.put(BLOCK_CLIENT_INVALID_BLOCK_ID, "Block id passed to block manager client should not be non-positive");

        // Block Manager Client
        ErrorCodeMap.put(BLOCK_MANAGER_CLIENT_CONNECT_FAILURE, "Failed to connect block manager client: ");
        ErrorCodeMap.put(NOT_BOUND_BLOCK_RMI, "Block manager RMI not bounded: ");
        ErrorCodeMap.put(BLOCK_MANAGER_CLIENT_REMOTE_EXCEPTION, "Block manager client remote exception:\n");
        ErrorCodeMap.put(NULL_BLOCK_MANAGER_CLIENT_ID, "Null block manager client id");
        ErrorCodeMap.put(EXISTING_BLOCK_MANAGER_CLIENT_ID, "Add existing block manager client id to controller: ");
        ErrorCodeMap.put(UNKNOWN_BLOCK_MANAGER_CLIENT_ID, "Unknown block manager client id argument to controller: ");
        ErrorCodeMap.put(BLOCK_CLIENT_INVALID_BLOCK_TYPE, "Invalid block id type passed to block manager client");

        // File Manager RMI
        ErrorCodeMap.put(NULL_FILE_RMI_MANAGER_ID_ARG, "Null manager id argument to file manager RMI");
        ErrorCodeMap.put(NULL_FILE_RMI_FIELD_ID_ARG, "Null field argument to file manager RMI");
        ErrorCodeMap.put(TERMINATE_UNKNOWN_FILE_RMI, "Try to terminate unknown file manager RMI service");
        ErrorCodeMap.put(TERMINATE_FILE_RMI_ERROR, "File manager RMI terminate error");
        ErrorCodeMap.put(NULL_FILE_RMI_FILE_ARG, "Null file argument to file manager RMI");
        ErrorCodeMap.put(INVALID_FILE_RMI_FILE_ARG, "Invalid file argument to file manager RMI");

        // File Manager Server
        ErrorCodeMap.put(NULL_FILE_MANAGER_SERVER_ID, "Null file manager id argument to controller");
        ErrorCodeMap.put(UNKNOWN_FILE_MANAGER_SERVER_ID, "Unknown file manager id argument to controller: ");
        ErrorCodeMap.put(EXISTING_FILE_MANAGER_SERVER_ID, "Add existing file manager id to controller: ");
        ErrorCodeMap.put(FILE_MANAGER_NOT_SERVING, "File manager is not serving: ");
        ErrorCodeMap.put(FILE_MANAGER_SERVER_LAUNCH_FAILURE, "Failed to launch file servers");
        ErrorCodeMap.put(BOUND_FILE_RMI_ERROR, "File manager RMI bound error");
        ErrorCodeMap.put(UNKNOWN_FILE_MANAGER_SERVER_EXCEPTION, "Unknown file manager server exception, server stop running: ");
        ErrorCodeMap.put(RESTART_FILE_MANAGER_SERVER_EXCEPTION, "Unknown file manager server exception, restart server: ");

        // File Manager Client Id
        ErrorCodeMap.put(NULL_FILE_MANAGER_CLIENT_ID_ARG, "Null argument passed to file manager client id");

        // File Manager Client
        ErrorCodeMap.put(FILE_MANAGER_CLIENT_CONNECT_FAILURE, "Failed to connect file manager client: ");
        ErrorCodeMap.put(NOT_BOUND_FILE_RMI, "File manager RMI not bounded: ");
        ErrorCodeMap.put(FILE_MANAGER_CLIENT_REMOTE_EXCEPTION, "File manager client remote exception:\n");
        ErrorCodeMap.put(NULL_FILE_MANAGER_CLIENT_ID, "Null file manager client id");
        ErrorCodeMap.put(EXISTING_FILE_MANAGER_CLIENT_ID, "Add existing file manager client id to controller: ");
        ErrorCodeMap.put(UNKNOWN_FILE_MANAGER_CLIENT_ID, "Unknown file manager client id argument to controller: ");
        ErrorCodeMap.put(FILE_CLIENT_INVALID_FIELD_TYPE, "Invalid field id type passed to file manager client");

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
