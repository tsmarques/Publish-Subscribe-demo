import java.net.ServerSocket;
import java.net.Socket;
import java.io.DataOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

public class EventBus {
    private static final String CMD_EXIT = "exit";
    private static final String CMD_SUBS = "subscribe";
    private static final String CMD_UNSUBS = "unsubscribe";
    private static final String CMD_LIST = "list";
    private static final String CMD_PUBL = "publish";
    private static final String CMD_UNPUBL = "unpublish";
    private static final String CMD_FORW = "forward";

    public static int PORT;
    private static ServerSocket server_s; // server socket
    private static HashMap<Socket, Publisher> publishers = new HashMap<>();
    private static HashMap<String, Publisher> streams = new HashMap<>(); // streams and its publishers
    
    public static void start_service() {
        new Thread() {
            public void run() {
                try {
                    System.out.println("# EventBus started...");
                    server_s  = new ServerSocket(PORT);
                    while(true) { // wait for a new connection and make a thread to "handle" it
                        Socket s = server_s.accept();
                        System.out.println("# Got a connection...");
                        handle_connection(s);
                    }
                } catch(Exception e) {e.printStackTrace();}
            }
        }.start(); 
    }

    private static void handle_connection(final Socket socket) {
        new Thread() {
            public void run() {
                try {
                    String client_msg;
                    BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                    DataOutputStream out = new DataOutputStream(socket.getOutputStream());
                    while(true) {
                        out.writeBytes("> ");
                        while((client_msg = in.readLine()) == null);
                        String cmd = client_msg.split(" ")[0];
                        if(cmd.equals(CMD_EXIT)) {
                            out.writeBytes("# Closing connection...\n");
                            if(publishers.containsKey(socket)) {
                                Publisher p = publishers.get(socket);
                                /* Erase this publisher streams  */
                                for(String stream : p.get_streams().keySet()) {
                                    if(streams.containsKey(stream))
                                        streams.remove(stream);
                                }
                            }
                            /* Check if this client is subscribed to streams */
                            for(Publisher publisher : publishers.values()) {
                                String subsribed_stream = publisher.has_subscriber(socket);
                                if(subsribed_stream != "")
                                    publisher.remove_subscriber(subsribed_stream, socket);
                            }
                            publishers.remove(socket);
                            socket.close();
                            break;
                        }
                        else if(cmd.equals(CMD_LIST))
                            list_streams(socket, out);
                        else {
                            String ret_v = "";
                            if(cmd.equals(CMD_PUBL)) {
                                ret_v = register_publisher(client_msg, socket);
                            }
                            else if(cmd.equals(CMD_UNPUBL)) {
                                ret_v = unregister_publisher(client_msg, socket);
                            }
                            else if(cmd.equals(CMD_SUBS)) {
                                ret_v = subscribe_stream(client_msg, socket);
                            }
                            else if(cmd.equals(CMD_UNSUBS)) {
                                ret_v = unsubscribe_stream(client_msg, socket);
                            }
                            else if(cmd.equals(CMD_FORW)) {
                                ret_v = forward(client_msg, socket);
                            }
                            out.writeBytes(ret_v);
                        }
                    }
                } catch(Exception e) {e.printStackTrace();}
            }
        }.start();
    }

    /***************************** Methods to handle publisher requests *****************************/
    /************************************************************************************************/
    private static String register_publisher(String client_msg, Socket socket) { // publish cmd
        try {
            String[] cmd = client_msg.split(" ");
            if(cmd.length == 2) {
                String stream = cmd[1];
                if(!stream_exists(stream)) {
                    Publisher p;
                    if(publishers.containsKey(socket))// if client is already a publisher
                        p = publishers.get(socket);
                    else {
                        p = new Publisher(socket);
                        publishers.put(socket, p);
                    }
                    p.add_stream(stream);
                    streams.put(stream, p);

                    return "# You're a publisher for this stream\n";
                }
                else
                    return "# There's already a publisher for this stream\n";
            }
            return "# Invalid command or arguments\n";
        } catch(Exception e) {
            e.printStackTrace();
        }
        return "# Invalid command or arguments\n";
    }

    private static String unregister_publisher(String client_msg, Socket socket) { // unpublish cmd
        String[] cmd = client_msg.split(" ");
        if(cmd.length == 2) {
            String stream = cmd[1];
            if(!stream_exists(stream))
                return "# This stream doesn't exist\n";
            else {
                if(publishers.containsKey(socket))  { // is a publisher
                    Publisher p = publishers.get(socket);
                    if(p.streams.containsKey(stream)) {
                        p.remove_stream(stream);
                        streams.remove(stream);
                        if(p.streams.size() == 0) // client not a publisher of anymore streams
                            publishers.remove(socket);
                        return "# You're not a publisher for this stream anymore\n";
                    }
                    else
                        return "# You're not a publisher for this stream\n"; // not a publisher of stream
                }
                else // not a publisher at all
                    return "# You're not a publisher\n";
            }
        }
        return "# Invalid command or arguments\n";
    }

    private static String forward(String client_msg, Socket socket) {
        String[] cmds = client_msg.split(" ");
        if(publishers.containsKey(socket)) {
            Publisher p = publishers.get(socket);
            if(cmds.length == 1) // send data to all streams of this Publisher
                p.forward();
            else if(cmds.length == 2) {
                String stream = cmds[1];
                if(stream_exists(stream)) {
                    if(p.streams.containsKey(stream)) {
                        p.forward(stream);
                        return "";
                    }
                    else
                        return "You're not a publisher for this stream\n";
                }
                else
                    return "# This stream doesn't exist\n";
            }
            else
               return "# Too many arguments\n";
        }
        else
            return "# You're not a publisher\n";

        return "";
    }

    /************************************************************************************************/
    /************************************************************************************************/


    /***************************** Methods to handle subscribers requests ***************************/
    /************************************************************************************************/

    private static String subscribe_stream(String client_msg, Socket socket) { // subscribe command
        try {
            String[] cmd = client_msg.split(" ");
            if(cmd.length == 2) {
                String stream = cmd[1];
                if(stream_exists(stream)) {
                    Publisher p = streams.get(stream);
                    if(p.has_subscriber(stream, socket)) // if client is already subscribed to this stream
                        return "# You're already subscribed to this stream\n";
                    p.add_subscriber(stream, socket);

                    return "# You're subscribed to this stream\n";
                }
                else
                    return "# This stream doesn't exist\n";
            }
            return "# Invalid command or arguments\n";
        } catch(Exception e) {
            e.printStackTrace();
        }
        return "# Invalid command or arguments\n";
    }
    
    private static String unsubscribe_stream(String client_msg, Socket socket) { // unsubscribe cmd
        try {
            String[] cmd = client_msg.split(" ");
            if(cmd.length == 2) {
                String stream = cmd[1];
                if(stream_exists(stream)) {
                    Publisher p = streams.get(stream);
                    if(!p.has_subscriber(stream, socket)) // if client is not subscriber of this stream
                        return "# You're not subscribed to this stream\n";
                    
                    p.remove_subscriber(stream, socket);
                    return "# You're not subscribed to this stream anymore\n";
                }
                else // stream doesn't exist
                    return "# This stream doesn't exist\n";
            }
            return "# Invalid command or arguments\n";
        } catch(Exception e) {
            e.printStackTrace();
        }
        return "# Invalid command or arguments\n";
    }
    /************************************************************************************************/
    /************************************************************************************************/


    private static void list_streams(Socket s, DataOutputStream out) {
        try {
            for(String stream : streams.keySet()) {
                out.writeBytes(stream + "\n");
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }

    // auxiliary function
    private static boolean stream_exists(String new_stream) {
        for(String stream : streams.keySet())
            if(new_stream.equals(stream))
                return true;
        return false;
    }
    
    public static void main(String []args) {
        EventBus.PORT = Integer.parseInt(args[0]);
        start_service();
    }
}
