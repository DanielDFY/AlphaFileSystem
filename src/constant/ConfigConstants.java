package constant;

public class ConfigConstants {
    public final static int BLOCK_SIZE = 512;
    public final static int DUPLICATION_NUM = 3;
    public final static int BLOCK_MANAGER_NUM = 5;
    public final static int FILE_MANAGER_NUM = 3;

    public final static String RMI_SERVER_HOST = "localhost";
    public final static int RMI_SERVER_PORT = 1099;
    public final static String RMI_BLOCK_MANAGER_REGISTRY_PREFIX = "rmi://";

    public final static int BLOCK_MANAGER_SERVER_BUFFER_SIZE = 10;
    public final static int BLOCK_MANAGER_CLIENT_BUFFER_SIZE = 10;

    public final static int BLOCK_MANAGER_CLIENT_SOCKET_TIMEOUT = 1000;
}
