// IN2011 Computer Networks
// Coursework 2023/2024
//
// Submission by
// Kateryna Storcheus
// 220053500
// kateryna.storcheus@city.ac.uk


import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

// DO NOT EDIT starts
interface FullNodeInterface {
    public boolean listen(String ipAddress, int portNumber);
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress);
}
// DO NOT EDIT ends


public class FullNode implements FullNodeInterface {
    private String name;
    private String address;
    private ServerSocket serverSocket;
    private ConcurrentMap<String, String> hashTable = new ConcurrentHashMap<>();
    private TreeMap<Integer, List<String>> networkMap = new TreeMap<>();

    public boolean listen(String ipAddress, int portNumber) {
        try {
            serverSocket = new ServerSocket(portNumber, 50, java.net.InetAddress.getByName(ipAddress));
            System.out.println("Server started on " + ipAddress + ":" + portNumber);
            this.address = ipAddress + ":" + portNumber;
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public void handleIncomingConnections(String startingNodeName, String startingNodeAddress) {
        this.name = startingNodeName;
        System.out.println("Handling connections for node: " + startingNodeName + " at " + startingNodeAddress);
        while (!serverSocket.isClosed()) {
            try {
                Socket clientSocket = serverSocket.accept();
                new Thread(() -> handleClient(clientSocket)).start(); // Handle each client in a new thread
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private void handleClient(Socket clientSocket) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream(), StandardCharsets.UTF_8));
             PrintWriter out = new PrintWriter(new OutputStreamWriter(clientSocket.getOutputStream(), StandardCharsets.UTF_8), true)) {
            String inputLine;
            while ((inputLine = in.readLine()) != null) { // Read lines from the client
                System.out.println("Received: " + inputLine);
                if (inputLine.startsWith("START")) {
                    out.println("START received");
                } else if (inputLine.startsWith("PUT?")) {
                    String[] parts = inputLine.split(" ");
                    int keyLines = Integer.parseInt(parts[1]);
                    int valueLines = Integer.parseInt(parts[2]);
                    StringBuilder key = new StringBuilder();
                    for (int i = 0; i < keyLines; i++) key.append(in.readLine()).append("\n");
                    StringBuilder value = new StringBuilder();
                    for (int i = 0; i < valueLines; i++) value.append(in.readLine()).append("\n");
                    hashTable.put(key.toString().trim(), value.toString().trim());
                    out.println("SUCCESS");
                } else if (inputLine.startsWith("GET?")) {
                    String[] parts = inputLine.split(" ");
                    int keyLines = Integer.parseInt(parts[1]);
                    StringBuilder key = new StringBuilder();
                    for (int i = 0; i < keyLines; i++) key.append(in.readLine()).append("\n");
                    String value = hashTable.get(key.toString().trim());
                    if (value != null) {
                        out.println("VALUE 1");
                        out.println(value);
                    } else {
                        out.println("NOPE");
                    }
                }else if (inputLine.startsWith("NOTIFY?")){
                    String nodeName = in.readLine();
                    String nodeAddress = in.readLine();
                    updateNetworkMap(calculateHashID(nodeName), nodeName);
                    out.println("NOTIFIED");
                }else if (inputLine.startsWith("NEAREST?")){
                    String hashID = inputLine.split(" ")[1];
                    SortedMap<Integer, List<String>> subMap = networkMap.headMap(calculateDistance(calculateHashID(this.name), hashID));
                    List<String> closestNodes = new ArrayList<>();
                    for (List<String> nodes : subMap.values()) {
                        closestNodes.addAll(nodes);
                        if (closestNodes.size() >= 3) break;
                    }
                    out.println("NODES " + Math.min(3, closestNodes.size()));
                    closestNodes.stream().limit(3).forEach(out::println);
                }
                else if (inputLine.equals("END")) {
                    out.println("Connection ending");
                    break;
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                clientSocket.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private static String calculateHashID(String text) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(text.getBytes(StandardCharsets.UTF_8));
            Formatter formatter = new Formatter();
            for (byte b : hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    private static int calculateDistance(String hashID1, String hashID2) {
        int distance = 256;
        int len = Math.min(hashID1.length(), hashID2.length());
        for (int i = 0; i < len; i++) {
            if (hashID1.charAt(i) != hashID2.charAt(i)) {
                // Calculate the number of matching bits at the start
                int bits = Integer.numberOfLeadingZeros(
                        Integer.parseInt(hashID1.substring(i, i + 1), 16) ^
                                Integer.parseInt(hashID2.substring(i, i + 1), 16)
                ) - 28; // 32 bits - 4 (as each hex digit represents 4 bits)
                distance = i * 4 + bits;
                break;
            }
        }
        return 256 - distance;
    }
    private void updateNetworkMap(String nodeHashID, String nodeName) {
        int distance = calculateDistance(calculateHashID(this.name), nodeHashID);
        List<String> nodesAtDistance = networkMap.getOrDefault(distance, new ArrayList<>());
        if (nodesAtDistance.size() < 3 || nodesAtDistance.contains(nodeName)) {
            if (!nodesAtDistance.contains(nodeName)) {
                nodesAtDistance.add(nodeName);
                if (nodesAtDistance.size() > 3) {
                    nodesAtDistance.remove(0);
                }
            }
            networkMap.put(distance, nodesAtDistance);
        }
    }
}
