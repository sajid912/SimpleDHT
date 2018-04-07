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

        try {
        String filename = values.getAsString("key");
        String hashedFileName = genHash(filename);
        Log.v("hashedFileName", hashedFileName);
        String value = values.getAsString("value") + "\n";

        FileOutputStream outputStream;
        outputStream = getContext().openFileOutput(filename, Context.MODE_PRIVATE);
        outputStream.write(value.getBytes());
        outputStream.close();

        } catch (Exception e) {
            Log.e(TAG, "File write failed");
        }

        return uri;
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        Log.v(TAG, "Content provider created!!");
        onNodeStart(getEmulatorId());
        return false;
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
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

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
            new clientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, EMULATOR0_PORT, constructJsonObject(), Constants.JOIN_REQUEST);

        }
    }

    private String constructJsonObject()
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
                        //Log.d(TAG,"Status is 0 and replying"+reply);
                        bufferedWriter.write(reply);
                        publishProgress(Constants.UPDATE_NEIGHBORS);

                    } else if(status == 2){
                        // Neighbor update msg
                        Log.d(TAG,"Status is 2");
                        updateMyNeighborsFromRemote(message);
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
            if(strings[0].equals(Constants.UPDATE_NEIGHBORS))
            {
                Log.v(TAG,"Updating neighbors");
                updateNeighbours(remoteNode, predecessorNode, successorNode);
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

        private void updateMyNeighborsLocally(int i, ArrayList<Node> list)
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
