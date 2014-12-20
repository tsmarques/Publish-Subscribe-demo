import java.net.Socket;
import java.io.DataOutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

class Publisher {
    private Socket socket;
    public HashMap<String, Stream> streams;
    
    public Publisher(Socket socket) {
        this.socket = socket;
        streams = new HashMap<>();
    }
    
    /*
      Send data through all streams.
      As this is just a demo, all the data are random floats.
    */
    public void forward() {
        try {
            Random rand = new Random();
            for(Stream stream : streams.values()) {
                for(Socket subscriber : stream.stream_subscribers()) {
                    /* Random data to simulate publications by this Publisher */
                    Float data1 = rand.nextFloat();
                    Float data2 = rand.nextFloat();
                    Float data3 = rand.nextFloat();
                    
                    DataOutputStream out = new DataOutputStream(subscriber.getOutputStream());
                    out.writeBytes("Stream data from: stream " + stream.category + "\n");
                    // send data to the subscribers
                    out.writeBytes(Float.toString(data1) + "\n");
                    out.writeBytes(Float.toString(data2) + "\n");
                    out.writeBytes(Float.toString(data3) + "\n");
                }
            }
        } catch(Exception e) { e.printStackTrace();}
    }

    /*
      Send data through a speicific stream.
      The data sent is like the function above.
     */
    public void forward(String category) {
        try {
            Random rand = new Random();
            Stream stream = streams.get(category);

            /* Random data to simulate publications by this Publisher */
            Float data1 = rand.nextFloat();
            Float data2 = rand.nextFloat();
            Float data3 = rand.nextFloat();
            
            for(Socket subscriber : stream.stream_subscribers()) {
                DataOutputStream out = new DataOutputStream(subscriber.getOutputStream());
                out.writeBytes("Stream data from: stream " + stream.category + "\n");
                // send data to the subscribers
                out.writeBytes(Float.toString(data1) + "\n");
                out.writeBytes(Float.toString(data2) + "\n");
                out.writeBytes(Float.toString(data3) + "\n");
            }
        } catch(Exception e) { e.printStackTrace();}
    }
    
    public LinkedList<Socket> get_subscribers(String stream) {
        return streams.get(stream).stream_subscribers();
    }

    public HashMap<String, Stream> get_streams() {
        return streams;
    }

    /* Search explicitly in a given stream if
       a given client/socket is subscribed to it.
       This function is used when the EventBus is
       checking if a client/socket is not already
       subscribed to this stream.
    */
    public boolean has_subscriber(String category, Socket socket) {
        return(get_subscribers(category).contains(socket));
        // for(Socket subs : get_subscribers(category)) {
        //     if(socket == subs) {
        //         return true;
        //     }
        // }
        // return false;
    }

    /*
      Search if a client/socket is subscribed to any stream.
      If it is, returns the name/category of the stream.
      This function is only used the EventBus when a client is disconnecting.
     */
    public String has_subscriber(Socket socket) {
        for(Stream stream : streams.values()) {
            if(has_subscriber(stream.category, socket))
                return stream.category;
        }
        return "";
    }

    public void add_stream(String category) {
        streams.put(category, new Stream(category));

    }

    public void remove_stream(String category) {
        streams.remove(category);
    }

    public void add_subscriber(String category, Socket subs) {
        Stream s = streams.get(category);
        s.stream_subscribers().add(subs);
    }

    public void remove_subscriber(String category, Socket subs) {
        Stream s = streams.get(category);
        s.stream_subscribers().remove(subs);
    }

    private class Stream {
        private String category;
        private LinkedList<Socket> subscribers; // maps a category of stream to a list of subscribers
        public Stream(String category) {
            this.category = category;
            subscribers = new LinkedList<>();
        }

        private LinkedList<Socket> stream_subscribers() {
            return subscribers;
        }
    }
}
