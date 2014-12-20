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

    public void forward() {
        try {
            Random rand = new Random();
            for(Stream stream : streams.values()) {
                for(Socket subscriber : stream.stream_subscribers()) {
                    DataOutputStream out = new DataOutputStream(subscriber.getOutputStream());
                    out.writeBytes("Stream data from: stream" + stream.category);
                    out.writeBytes(Float.toString(rand.nextFloat()));
                }
            }
        } catch(Exception e) { e.printStackTrace();}
    }
    
    public LinkedList<Socket> get_subscribers(String stream) {
        return streams.get(stream).stream_subscribers();
    }

    public boolean has_subscriber(String category, Socket socket) {
        for(Socket subs : get_subscribers(category)) {
            if(socket.getChannel() == subs.getChannel()) {
                System.out.println("PIM");
                return true;
            }
        }
        return false;
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
