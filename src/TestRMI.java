import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMISocketFactory;
import java.rmi.server.UnicastRemoteObject;
import java.util.Scanner;

public class TestRMI {
    public static class TestClass implements Serializable {
        private static final long serialVersionUID = -4335989759417551528L;

        public double i;

        public TestClass(double i) {
            this.i = i;
        }
    }

    public interface IRemoteMath extends Remote {
        TestClass add(TestClass a, TestClass b) throws RemoteException;
        TestClass subtract(TestClass a, TestClass b) throws RemoteException;
    }

    public static class RemoteMath extends UnicastRemoteObject implements IRemoteMath {

        private int numberOfComputations;

        protected RemoteMath() throws RemoteException {
            numberOfComputations = 0;
        }

        @Override
        public TestClass add(TestClass a, TestClass b) {
            numberOfComputations++;
            System.out.println("Number of computations performed so far = "
                    + numberOfComputations);
            return new TestClass(a.i+b.i);
        }

        @Override
        public TestClass subtract(TestClass a, TestClass b) {
            numberOfComputations++;
            System.out.println("Number of computations performed so far = "
                    + numberOfComputations);
            try {
                Thread.sleep(500);
            } catch (Exception e) {
                e.printStackTrace();
            }
            return new TestClass(a.i-b.i);
        }
    }

    public static class RMIServer {
        public void run() {
            try {
                IRemoteMath remoteMath = new RemoteMath();
                LocateRegistry.createRegistry(1099);
                Registry registry = LocateRegistry.getRegistry();
                registry.bind("Compute", remoteMath);
                System.out.println("Math server ready");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class MathClient {
        static {
            try {
                RMISocketFactory.setSocketFactory(new RMISocketFactory()
                {
                    public Socket createSocket(String host, int port) throws IOException {
                        Socket socket = new Socket(host, port);
                        socket.setSoTimeout(1000);
                        socket.setSoLinger(false, 0);
                        return socket;
                    }
                    public ServerSocket createServerSocket(int port) throws IOException {
                        return new ServerSocket(port);
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void run(String arg) {
            try {
                Registry registry = LocateRegistry.getRegistry("localhost");
                IRemoteMath remoteMath = (IRemoteMath)registry.lookup("Compute");

                System.out.println("Client: " + arg);
                double addResult = remoteMath.add(new TestClass(5.0), new TestClass(3.0)).i;
                System.out.println("5.0 + 3.0 = " + addResult);
                double subResult = remoteMath.subtract(new TestClass(5.0), new TestClass(3.0)).i;
                System.out.println("5.0 - 3.0 = " + subResult);
            } catch (Exception e) {
                e.printStackTrace();
            }
            if (arg.equals("exception"))
                throw new RuntimeException("client");
        }
    }

    public static void main(String[] args) throws IOException {
        RMIServer server = new RMIServer();
        server.run();

        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String arg = scanner.nextLine();
            MathClient client = new MathClient();
            try {
                client.run(arg);
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }

}
