// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// YOUR_NAME_GOES_HERE
// YOUR_STUDENT_ID_NUMBER_GOES_HERE
// YOUR_EMAIL_GOES_HERE


import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Map;


// DO NOT EDIT starts
interface TemporaryNodeInterface {
    public boolean start(String startingNodeName, String startingNodeAddress);
    public boolean store(String key, String value);
    public String get(String key);
}
// DO NOT EDIT ends


public class TemporaryNode implements TemporaryNodeInterface {
    private BufferedReader in;
    private Socket ConnectionSocket;
    private Writer out;
    private Map<String, String> simulatedNetworkStorage;
    private String startingNodeName;
    private String startingNodeAddress;



    public boolean start(String startingNodeName, String startingNodeAddress) {
	// Implement this!
	// Return true if the 2D#4 network can be contacted
        this.startingNodeName = startingNodeName;
        try {
            //Connection
            ConnectionSocket = new Socket();
            ConnectionSocket.connect(new InetSocketAddress(startingNodeAddress.split(":")[0], Integer.parseInt(startingNodeAddress.split(":")[1])), 5000);
            out = new OutputStreamWriter(ConnectionSocket.getOutputStream());
            in = new BufferedReader(new InputStreamReader(ConnectionSocket.getInputStream()));
            //Sending START
            out.write("START 1 " + startingNodeName + "\n");
            out.flush();
            //response
            String response = in.readLine();

            //check if START
            if (response.startsWith("START")) {
                return true;
            }
        }
            catch (Exception e){
                e.printStackTrace();

            }
        // Return false if the 2D#4 network can't be contacted
        return false;
        }


    public boolean store(String key, String value) {
        // Implement this!
        // Return true if the store worked
        try {
            //PUT request
            out.write("PUT? 1 1" + "\n");
            out.flush();
            out.write(key);
            out.flush();
            out.write(value);
            out.flush();
            //response
            String response = in.readLine();
            //check
            if (response.equals("SUCCESS")) {
                return true;
            }
        }
        catch(Exception e){
                e.printStackTrace();
            }
            // Return false if the store failed
            return false;
    }

    public String get(String key) {
	// Implement this!
	// Return the string if the get worked
//Send GET
        try{
        out.write("GET? 1" + "\n");
        out.flush();
        out.write(key);
        out.flush();
       String response = in.readLine();
        if (response.startsWith("VALUE")) {
            StringBuilder value = new StringBuilder();
            int numberLines = Integer.parseInt(response.split(" ")[1]);
            for (int i = 0; i < numberLines; i++) {
                String Line = in.readLine();
                value.append(Line + "\n");
            }
            System.out.println(value);
            return value.toString();
            // Return null if it didn't
            } else if (response.equals("NOPE")) {
            return null;
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
        //return "Not implemented";
        return "Not Implemented";
}
    public void echo() {
        try {
            out.write("ECHO?" + "\n");
            out.flush();
            System.out.println("Echo:");
            String response;
            while ((response = in.readLine()) != null) {
                System.out.println(response);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}


