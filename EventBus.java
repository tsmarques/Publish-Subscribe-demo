import java.net.ServerSocket;
import java.net.Socket;
import java.io.DataOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

/*
  Implement forward function
*/
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
                        else if(cmd.equals(CMD_PUBL)) {
                            int ret_v = register_publisher(client_msg, socket);
                            if(ret_v == 1)
                                out.writeBytes("# You're a publisher for this stream\n");
                            else if(ret_v == 0)
                                out.writeBytes("# There's already a publisher for stream " + client_msg.split(" ")[1] + "\n");
                            else
                                out.writeBytes("# Invalid command or arguments\n");
                        }
                        else if(cmd.equals(CMD_UNPUBL)) {
                            int ret_v = unregister_publisher(client_msg, socket);
                            if(ret_v == 1)
                                out.writeBytes("# You're not a publisher for this stream anymore\n");
                            else if(ret_v == 0)
                                out.writeBytes("# You're not a publisher for this stream\n");
                            else if(ret_v == -1)
                                out.writeBytes("# This stream doesn't exist\n");
                            else if(ret_v == -2)
                                out.writeBytes("# You're not a publisher\n");
                            else if(ret_v == -3)
                                out.writeBytes("# Invalid command or arguments\n");
                        }
                        else if(cmd.equals(CMD_SUBS)) {
                            int ret_v = subscribe_stream(client_msg, socket);
                            if(ret_v == 1)
                                out.writeBytes("# You're subscribed to this stream\n");
                            else if(ret_v == 0)
                                out.writeBytes("# You're already subscribed to this stream\n"); 
                            else if(ret_v == -1)
                                out.writeBytes("# This stream doesn't exist\n");
                            else if(ret_v == -2)
                                out.writeBytes("# Invalid command or arguments\n");
                        }
                        else if(cmd.equals(CMD_UNSUBS)) {
                            int ret_v = unsubscribe_stream(client_msg, socket);
                            if(ret_v == 1)
                                out.writeBytes("# You're not subscribed to this stream anymore\n");
                            else if(ret_v == 0)
                                out.writeBytes("# You're not subscribed to this stream\n");
                            else if(ret_v == -1)
                                out.writeBytes("# This stream doesn't exist\n");
                            else if(ret_v == -2)
                                out.writeBytes("# Invalid command or arguments\n");
                        }
                        else if(cmd.equals(CMD_FORW)) {
                            String[] cmds = client_msg.split(" ");
                            if(publishers.containsKey(socket)) {
                                Publisher p = publishers.get(socket);
                                if(cmds.length == 1) // send data to all streams of this Publisher
                                    p.forward();
                                else if(cmds.length == 2) {
                                    String stream = cmds[1];
                                    if(stream_exists(stream)) {
                                        if(p.streams.containsKey(stream))
                                            p.forward(stream);
                                        else
                                            out.writeBytes("# You're not a publisher for this stream\n");
                                    }
                                    else
                                        out.writeBytes("# This stream doesn't exist\n");
                                }
                                else
                                    out.writeBytes("# Too many arguments\n");
                            }
                            else
                                out.writeBytes("# You're not a publisher\n");
                        }
                    }
                } catch(Exception e) {e.printStackTrace();}
            }
        }.start();
    }
    
    private static int register_publisher(String client_msg, Socket socket) {
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

                    return 1;
                }
                else
                    return 0;
            }
            return -1;
        } catch(Exception e) {
            e.printStackTrace();
        }
        return -1;
    }

    private static int unregister_publisher(String client_msg, Socket socket) {
        String[] cmd = client_msg.split(" ");
        if(cmd.length == 2) {
            String stream = cmd[1];
            if(!stream_exists(stream))
                return -1;
            else {
                if(publishers.containsKey(socket))  { // is a publisher
                    Publisher p = publishers.get(socket);
                    if(p.streams.containsKey(stream)) {
                        p.remove_stream(stream);
                        streams.remove(stream);
                        if(p.streams.size() == 0) // client not a publisher of anymore streams
                            publishers.remove(socket);
                        return 1;
                    }
                    else
                        return 0; // not a publisher of stream
                }
                else // not a publisher at all
                    return -2;
            }
        }
        return -3;
    }


    private static void list_streams(Socket s, DataOutputStream out) {
        try {
            for(String stream : streams.keySet()) {
                out.writeBytes(stream + "\n");
            }
        } catch(Exception e) {
            e.printStackTrace();
        }
    }



    private static int subscribe_stream(String client_msg, Socket socket) {
        try {
            String[] cmd = client_msg.split(" ");
            if(cmd.length == 2) {
                String stream = cmd[1];
                if(stream_exists(stream)) {
                    Publisher p = streams.get(stream);
                    if(p.has_subscriber(stream, socket)) // if client is already subscribed to this stream
                        return 0;
                    p.add_subscriber(stream, socket);

                    return 1;
                }
                else
                    return -1;
            }
            return -2;
        } catch(Exception e) {
            e.printStackTrace();
        }
        return -2;
    }
    // NOT WORKING WHEN TRYING TO UNSUBSCRIBE A STREAM(has_subscriber PROB. NOT WORKING)
    private static int unsubscribe_stream(String client_msg, Socket socket) {
        try {
            String[] cmd = client_msg.split(" ");
            if(cmd.length == 2) {
                String stream = cmd[1];
                if(stream_exists(stream)) {
                    Publisher p = streams.get(stream);
                    if(!p.has_subscriber(stream, socket)) // if client is not subscriber of this stream
                        return 0;
                    
                    p.remove_subscriber(stream, socket);
                    return 1;
                }
                else // stream doesn't exist
                    return -1;
            }
            return -2;
        } catch(Exception e) {
            e.printStackTrace();
        }
        return -2;
    }

    
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
