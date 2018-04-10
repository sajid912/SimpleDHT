package edu.buffalo.cse.cse486586.simpledht;

/**
 * Created by sajidkhan on 4/1/18.
 */

public class Constants {

    public static final String STATUS = "status";
    public static final String PREDECESSOR_PORT = "predecessorPort";
    public static final String SUCCESSOR_PORT = "successorPort";

    public static final String MY_PORT = "my_port";
    public static final String MY_NODE_ID = "nodeId";

    public static final String JOIN_REQUEST = "joinRequest";
    public static final String UPDATE_NEIGHBORS = "updateNeighbors";

    public static final String UPDATE_NEIGHBORS_LOCAL = "updateNeighborsLocally";
    public static final String UPDATE_NEIGHBORS_REMOTE = "updateNeighborsRemote";
    public static final String DATA_FORWARD_REQUEST = "forwardDataRequest";
    public static final String DATA_QUERY_REQUEST = "dataQueryRequest";
    public static final String DATA_QUERY_REPLY = "dataQueryReply";

    public static final String ALL_DATA_QUERY_REQUEST = "allDataQueryRequest";
    public static final String ALL_DATA_QUERY_REPLY = "allDataQueryReply";

    public static final String DATA_ORIGIN_PORT = "dataOriginPort";
    public static final String QUERY_ORIGIN_PORT = "queryOriginPort";
    public static final String KEY = "key";
    public static final String VALUE = "value";

    public static final String EMULATOR0_PORT = "11108";
    public static final String EMULATOR1_PORT = "11112";
    public static final String EMULATOR2_PORT = "11116";
    public static final String EMULATOR3_PORT = "11120";
    public static final String EMULATOR4_PORT = "11124";


    public static final int SERVER_PORT = 10000;

    public static final String[] ALL_PORTS = {EMULATOR0_PORT, EMULATOR1_PORT, EMULATOR2_PORT, EMULATOR3_PORT, EMULATOR4_PORT };


    public static final String COUNT = "count";
    public static final String TEXT_SEPARATOR = "---";
    public static final String KEY_VALUE_SEPARATOR = ";";
    public static final String ALL_DATA_QUERY_CONTENT = "allDataQueryContent";
    public static final String NODE_LIST_CONTENT = "nodeListContent";
    public static final String NODE_LIST_QUERY_REQUEST = "nodeListQueryRequest";
    public static final String NODE_LIST_QUERY_REPLY = "nodeListQueryReply";

    public static final String ALL_DATA_DELETE_REQUEST = "allDataDeleteRequest";
    public static final String SINGLE_DATA_DELETE_REQUEST = "singleFileDeleteRequest";

}
