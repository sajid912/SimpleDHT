package edu.buffalo.cse.cse486586.simpledht;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Formatter;
import java.util.PriorityQueue;
import java.util.Random;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.provider.MediaStore;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.json.JSONException;
import org.json.JSONObject;

import static android.content.ContentValues.TAG;

//Edited by sajid again
public class SimpleDhtProvider extends ContentProvider {


    private static String EMULATOR_0_HASH = "";
    private static String EMULATOR_1_HASH = "";
    private static String EMULATOR_2_HASH = "";
    private static String EMULATOR_3_HASH = "";
    private static String EMULATOR_4_HASH = "";
    private static String CHECK = "";

    private static final String EMULATOR0_PORT = "11108";
    private ArrayList<Node> ringList = new ArrayList<Node>();
    private Node my_node;

//    private String MY_PORT="";
//    private String MY_NODE_ID="";
    private String MY_PREDECESSOR_PORT = "";
    private String MY_SUCCESSOR_PORT = "";

    private static final int SERVER_PORT = 10000;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        Log.v("delete", selection);

        try {

            if(selection.equals("*")) // delete all key value pairs in entire DHT
            {
                for(File file: getContext().getFilesDir().listFiles())
                {
                    file.delete();
                }

            }
            else if(selection.equals("@")) // delete key value pairs in the local node
            {
                for(File file: getContext().getFilesDir().listFiles())
                {
                   file.delete();
                }

            }
            else // other cases
            {
                 getContext().getFileStreamPath(selection).delete();
            }

        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        Log.v("insert", values.toString());
        String key = values.getAsString("key");
        String value = values.getAsString("value");

        insertValues(key, value);
        return uri;
    }

    private void insertValues(String key, String value)
    {

        boolean forward = false;

        if(MY_PREDECESSOR_PORT.isEmpty() && MY_SUCCESSOR_PORT.isEmpty())
        {
            // Node ring hasn't formed yet, so save locally
        }
        else
        {
            String my_nodeId = my_node.getNodeId();
            int pre_p = Integer.parseInt(MY_PREDECESSOR_PORT);
            int pre_id = pre_p/2;
            String preIdStr = String.valueOf(pre_id);
            String my_preId = genHash(preIdStr);
            //String my_sucId = genHash(MY_SUCCESSOR_PORT);

            checkValues(key);
            Log.d(TAG,"Key:"+key);
            String hashedKey = genHash(key);

            int comparision = hashedKey.compareTo(my_nodeId);
            Log.d(TAG,"Comparision of hashedKey and my node id:"+comparision);

            if(hashedKey.compareTo(my_nodeId)>0)
            {
                int c1= my_preId.compareTo(my_nodeId);
                int c2= hashedKey.compareTo(my_preId);
                Log.d(TAG, "Comparision of pred and my node id:"+c1);
                Log.d(TAG, "Comparision of hashedKey and pred:"+c2);

                if(my_preId.compareTo(my_nodeId)>0 && hashedKey.compareTo(my_preId)>0) // My pre is greater than me && key is greater than my pre
                {
                    // save the key locally
                    Log.d(TAG, "Higher than large node, save it");
                    //Log.d(TAG, "HashedKey:"+hashedKey+" my_nodeId:"+my_nodeId+" my_predId:"+my_preId);

                }
                else
                {
                    //Log.d(TAG, "HashedKey:"+hashedKey+" my_nodeId:"+my_nodeId+" my_predId:"+my_preId);

                    forward = true; // Forward to successor
                    Log.d(TAG,"Key greater than me, forward to successor");
                }

            } else if(hashedKey.compareTo(my_nodeId)<0)
            {
                int c1= my_preId.compareTo(my_nodeId);
                int c2= hashedKey.compareTo(my_preId);
                Log.d(TAG, "Comparision of pred and my node id:"+c1);
                Log.d(TAG, "Comparision of hashedKey and pred:"+c2);

                if(hashedKey.compareTo(my_preId)>0)
                {
                    // Store the value locally
                    Log.d(TAG, "Key less than me, greater than pred, save it");
                    //Log.d(TAG, "HashedKey:"+hashedKey+" my_nodeId:"+my_nodeId+" my_predId:"+my_preId);

                }
                else if(my_preId.compareTo(my_nodeId)>0) // My pre is greater than me && key is less than me
                {
                    // save locally
                    Log.d(TAG, "Smaller than small node, so save it");
                    //Log.d(TAG, "HashedKey:"+hashedKey+" my_nodeId:"+my_nodeId+" my_predId:"+my_preId);

                }
               else
                {
                    //Log.d(TAG, "HashedKey:"+hashedKey+" my_nodeId:"+my_nodeId+" my_predId:"+my_preId);
                    forward = true;
                    Log.d(TAG,"Key smaller than me and my pred, so forward");
                }

            }
        }


        if(forward)
        {
            Log.d(TAG,"Forwarding the request");
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, MY_SUCCESSOR_PORT,
                    constructDataObject(key, value), Constants.DATA_FORWARD_REQUEST);
        }
        else
        {
            Log.d(TAG,"Trying to save locally");
            try {
                value = value + "\n";
                FileOutputStream outputStream;
                outputStream = getContext().openFileOutput(key, Context.MODE_PRIVATE);
                outputStream.write(value.getBytes());
                outputStream.close();

            } catch (Exception e) {
                Log.e(TAG, "File write failed");
            }
        }

    }

    private String constructDataObject(String key, String value)
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 3); // 3 stands for data forward among nodes
            jsonObject.put(Constants.DATA_ORIGIN_PORT, my_node.getPortNo());
            jsonObject.put(Constants.KEY, key); // port no of predecessor
            jsonObject.put(Constants.VALUE, value); // port no of successor
            jsonObject.put(Constants.MY_PORT, my_node.getPortNo());
            //jsonObject.put(Constants.COUNT, count);

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Log.v(TAG, "Content provider created!!");
        onNodeStart(getEmulatorId());
        return false;
    }

    private void checkValues(String key)
    {
        EMULATOR_0_HASH = genHash("5554");
        EMULATOR_1_HASH = genHash("5556");
        EMULATOR_2_HASH = genHash("5558");
        EMULATOR_3_HASH = genHash("5560");
        EMULATOR_4_HASH = genHash("5562");
        String hash = genHash(key);

        Log.d(TAG, hash.compareTo(EMULATOR_4_HASH)+" check0");
        Log.d(TAG, hash.compareTo(EMULATOR_1_HASH)+" check1");
        Log.d(TAG, hash.compareTo(EMULATOR_0_HASH)+" check2");
        Log.d(TAG, hash.compareTo(EMULATOR_2_HASH)+" check3");
        Log.d(TAG, hash.compareTo(EMULATOR_3_HASH)+" check4");
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
            String sortOrder) {
        // TODO Auto-generated method stub

        Log.v("query", selection);

        try {

            if(selection.equals("*")) // return all key value pairs in entire DHT
            {
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

                for(File file: getContext().getFilesDir().listFiles())
                {
                    String fileName = file.getName();
                    String fileValue = getFileContentFromName(fileName);
                    matrixCursor.addRow(new Object[]{fileName, fileValue});
                }

                return matrixCursor;
            }
            else if(selection.equals("@")) // return key value pairs in the local node
            {
                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});

                for(File file: getContext().getFilesDir().listFiles())
                {
                    String fileName = file.getName();
                    String fileValue = getFileContentFromName(fileName);
                    matrixCursor.addRow(new Object[]{fileName, fileValue});
                }

                return matrixCursor;
            }
            else // other cases
            {
                String value = getFileContentFromName(selection);

                MatrixCursor matrixCursor = new MatrixCursor(new String[]{"key", "value"});
                matrixCursor.addRow(new Object[]{selection, value});
                return matrixCursor;
            }

        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    private String genHash(String input){
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            byte[] sha1Hash = sha1.digest(input.getBytes());
            Formatter formatter = new Formatter();
            for (byte b : sha1Hash) {
                formatter.format("%02x", b);
            }
            return formatter.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
      return "";
    }


    private String getFileContentFromName(String fileName)
    {

        try {
            FileInputStream inputStream = getContext().openFileInput(fileName);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String value = bufferedReader.readLine();
            Log.d(TAG, "Value is:" + value);
            bufferedReader.close();
            return value;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
       return "";
    }

    private String getEmulatorId()
    {
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        return String.valueOf((Integer.parseInt(portStr)));
    }

    public String getPortNumber(String emulatorId)
    {
        int a = Integer.parseInt(emulatorId);
        int port_no = 2*a;
        return String.valueOf(port_no);
    }

    private void onNodeStart(String emulatorId)
    {
        try {
            //ServerSocket serverSocket = new ServerSocket(SERVER_PORT);

            ServerSocket serverSocket = new ServerSocket(); // <-- create an unbound socket first
            serverSocket.setReuseAddress(true);
            serverSocket.bind(new InetSocketAddress(SERVER_PORT));
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
            Log.d(TAG, "Creating socket now");

        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket");
            e.printStackTrace();
            return;
        }

        my_node = new Node(genHash(emulatorId), getPortNumber(emulatorId));
        ringList.add(my_node);

        if(!my_node.getPortNo().equals(EMULATOR0_PORT))
        {
          //  All emulators except 5554 will request 5554 to join DHT
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, EMULATOR0_PORT, constructNodeJoinObject(), Constants.JOIN_REQUEST);

        }
    }

    private String constructNodeJoinObject()
    {
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 0); // 0 stands for DHT join request
            jsonObject.put(Constants.PREDECESSOR_PORT, "0"); // port no of predecessor
            jsonObject.put(Constants.SUCCESSOR_PORT, "0"); // port no of successor
            jsonObject.put(Constants.MY_PORT, my_node.getPortNo());

        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }

    private class clientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {

            try {

                if(!msgs[0].equals(my_node.getPortNo()))
                {
                    Socket socket = new Socket();

                    socket.connect(new InetSocketAddress(InetAddress.getByAddress(new byte[]{(byte) 10, (byte) 0, (byte) 2, (byte) 2}),
                            Integer.parseInt(msgs[0])));

                    String msgToSend = msgs[1];

                    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                    out.println(msgToSend);

                    Log.d(TAG, "clientTask to port" + msgToSend + " : " + msgs[0]);

                    if(msgs[2].equals(Constants.JOIN_REQUEST))
                    {
                        InputStream inputStream = socket.getInputStream();
                        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                        String reply = bufferedReader.readLine();
                        Log.d(TAG, "Reply status:" + reply);
                        handleReply(reply);

                        bufferedReader.close();
                    }

                    out.close();
                    socket.close();
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (SocketException e) {
                Log.e(TAG, "Socket Exception, " + msgs[0] + " has failed");
                e.printStackTrace();
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
                e.printStackTrace();
            } catch (Exception e) {
                Log.e(TAG, "General exception");
                e.printStackTrace();
            }

            return null;
        }

        protected void handleReply(String reply)
        {
            try {
                JSONObject jsonObject = new JSONObject(reply);
                int status = jsonObject.getInt(Constants.STATUS);
                if(status == 1)
                {
                    MY_PREDECESSOR_PORT = jsonObject.getString(Constants.PREDECESSOR_PORT);
                    MY_SUCCESSOR_PORT = jsonObject.getString(Constants.SUCCESSOR_PORT);
                }


            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        private Node remoteNode;
        private Node predecessorNode;
        private Node successorNode;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            String message;
            Log.d(TAG, "Server task");

            while (true) {
                try {
                    Socket socket = serverSocket.accept();

                    InputStream inputStream = socket.getInputStream();
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                    message = bufferedReader.readLine();

                    Log.d(TAG,"Message received:"+message);
                    OutputStream outputStream = socket.getOutputStream();
                    BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(outputStream));

                    JSONObject jsonObject = new JSONObject(message);
                    int status = jsonObject.getInt(Constants.STATUS);
                    if (status == 0) {
                        String reply = handleNodeJoinRequest(message);
                        Log.d(TAG,"Status is 0");
                        bufferedWriter.write(reply);
                        publishProgress(Constants.UPDATE_NEIGHBORS_LOCAL);

                    } else if(status == 2){
                        // Neighbor update msg
                        Log.d(TAG,"Status is 2");
                        publishProgress(Constants.UPDATE_NEIGHBORS_REMOTE, message);

                    } else if(status == 3){
                        // Data forward request
                        Log.d(TAG,"Status is 3");
                        publishProgress(Constants.DATA_FORWARD_REQUEST, message);
                    }

                    bufferedWriter.flush();
                    bufferedWriter.close();

                    bufferedReader.close();
                    socket.close();
                } catch (SocketTimeoutException e) {
                    Log.d(TAG, "Socket time out occurred");
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (JSONException e) {
                    e.printStackTrace();
                }
                //return null;
            }
        }

        protected void onProgressUpdate(String... strings) {

            //Log.d(TAG, "Received:" + strings[0] + " My port:" + myPort);
            if(strings[0].equals(Constants.UPDATE_NEIGHBORS_LOCAL))
            {
                Log.v(TAG,"Updating neighbors locally");
                updateNeighbours(remoteNode, predecessorNode, successorNode);
            }
            else if(strings[0].equals(Constants.UPDATE_NEIGHBORS_REMOTE))
            {
                Log.v(TAG,"Updating neighbors remotely");
                updateMyNeighborsFromRemote(strings[1]);
            }
            else if(strings[0].equals(Constants.DATA_FORWARD_REQUEST))
            {
                Log.v(TAG,"Data forward request handling");
                handleDataForwardRequest(strings[1]);

            }

            return;
        }

        protected String handleNodeJoinRequest(String object) {
            try {

                Log.d(TAG, "Received join request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String remotePortStr = jsonObject.getString(Constants.MY_PORT);

                int remotePort = Integer.parseInt(remotePortStr); // remote port
                int remoteEmulatorId = remotePort/2; // emulator_id from port
                String remoteEmulatorIdStr = String.valueOf(remoteEmulatorId);

                remoteNode = new Node(genHash(remoteEmulatorIdStr), remotePortStr);
                ringList.add(remoteNode);

                Collections.sort(ringList);
                ArrayList<Node> list = new ArrayList<Node>(ringList);
                int index=0;

                for(int i=0;i<list.size();i++)
                {
                    Log.d(TAG, "List port:"+list.get(i).getPortNo()+" index:"+i);

                    String nodeId = list.get(i).getNodeId();

                    if(nodeId.equals(my_node.getNodeId())) // This is to update pre and suc of 5554 node
                        updateMyNeighborsLocally(i, list);

                    if(nodeId.equals(remoteNode.getNodeId()))
                       {
                           index=i;
                           break;
                       }
                }

                if(index == 0)
                {
                    // Predecessor is list.size()-1, Successor is index+1
                    predecessorNode = list.get(list.size()-1);
                    successorNode = list.get(index+1);
                }
                else if(index == list.size()-1)
                {
                    // Predecessor is index-1, successor is index 0
                    predecessorNode = list.get(index-1);
                    successorNode = list.get(0);
                }
                else
                {
                    // For any other index, predecessor is index-1 and successor is index+1
                    predecessorNode = list.get(index-1);
                    successorNode = list.get(index+1);
                }

                jsonObject.put(Constants.STATUS, 1); // 1 is reply to join request
                jsonObject.put(Constants.PREDECESSOR_PORT, predecessorNode.getPortNo());
                jsonObject.put(Constants.SUCCESSOR_PORT, successorNode.getPortNo());
                jsonObject.put(Constants.MY_PORT, my_node.getPortNo());
                return jsonObject.toString();

            } catch (JSONException e) {
                e.printStackTrace();
            }
            return "";
        }

        protected void updateMyNeighborsLocally(int i, ArrayList<Node> list)
        {
            if(i == 0)
            {
                // Predecessor is list.size()-1, Successor is index+1
                MY_PREDECESSOR_PORT = list.get(list.size()-1).getPortNo();
                MY_SUCCESSOR_PORT = list.get(i+1).getPortNo();
            }
            else if(i == list.size()-1)
            {
                // Predecessor is index-1, successor is index 0
                MY_PREDECESSOR_PORT = list.get(i-1).getPortNo();
                MY_SUCCESSOR_PORT = list.get(0).getPortNo();
            }
            else
            {
                // For any other index, predecessor is index-1 and successor is index+1
                MY_PREDECESSOR_PORT = list.get(i-1).getPortNo();
                MY_SUCCESSOR_PORT = list.get(i+1).getPortNo();
            }
        }

        protected void updateMyNeighborsFromRemote(String object)
        {
            try {

                Log.d(TAG, "Update neighbor:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String newJoineePort = jsonObject.getString(Constants.MY_PORT);
                String newJoinee_p = jsonObject.getString(Constants.PREDECESSOR_PORT);
                String newJoinee_s = jsonObject.getString(Constants.SUCCESSOR_PORT);

                if(my_node.getPortNo().equals(newJoinee_p))
                {
                    // My successor will be newJoinee
                    MY_SUCCESSOR_PORT = newJoineePort;
                }
                else if(my_node.getPortNo().equals(newJoinee_s))
                {
                    // My predecessor will be newJoinee
                    MY_PREDECESSOR_PORT = newJoineePort;
                }

                Log.v(TAG,"My port:"+my_node.getPortNo()+" MY Pre:"+MY_PREDECESSOR_PORT+" My Suc:"+MY_SUCCESSOR_PORT);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }

        protected void handleDataForwardRequest(String object)
        {
            try {

                Log.d(TAG, "Data forward request:" + object);

                JSONObject jsonObject = new JSONObject(object);
                String key = jsonObject.getString(Constants.KEY);
                String value = jsonObject.getString(Constants.VALUE);
                //int  count = jsonObject.getInt(Constants.COUNT);

                insertValues(key, value);

            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
    }

    private void updateNeighbours(Node r, Node p, Node s)
    {
        String[] neighbourPorts = {p.getPortNo(),s.getPortNo()};
        for(String port : neighbourPorts)
        {
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, port, constructNeighborUpdateObject(r, p, s), Constants.UPDATE_NEIGHBORS);
        }
    }

    private String constructNeighborUpdateObject(Node r, Node p, Node s)
    {
        // Details of the node which joined
        JSONObject jsonObject = new JSONObject();

        try {
            jsonObject.put(Constants.STATUS, 2); // 2 for neighbor update
            jsonObject.put(Constants.PREDECESSOR_PORT, p.getPortNo()); // node id of predecessor
            jsonObject.put(Constants.SUCCESSOR_PORT, s.getPortNo()); // node id of successor
            jsonObject.put(Constants.MY_PORT, r.getPortNo()); // port of node which joined
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return jsonObject.toString();
    }
}
