import static java.lang.System.out;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Main{

    private static final ConcurrentHashMap<String, TimedValue> timedMap = new ConcurrentHashMap<>();

    public static Map<String, String> config = new HashMap<String, String>();
    public static Map<String, String> localMap = new HashMap<String, String>();

    public static void main(String[] args){

        for(int i = 0; i < args.length; i++){
            String arg = args[i];
            if(arg.equals("--dir")){
                config.put("dir", args[++i]);
            }
            if(arg.equals("--dbfilename")){
                config.put("dbfilename", args[++i]);
            }
        }

        try{
            readRDBFile();
        }catch(IOException e){
            out.println("IOException in read RDB File: " + e.getMessage());
        }


        try(Selector selector = Selector.open();
            ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()){

            serverSocketChannel.bind(new InetSocketAddress(6379));
            serverSocketChannel.configureBlocking(false);
            serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

            while(true){
                selector.select(); // 阻塞直到有事件发生
                Set<SelectionKey> selectedKeys = selector.selectedKeys();
                Iterator<SelectionKey> iterator = selectedKeys.iterator();

                while(iterator.hasNext()){
                    SelectionKey key = iterator.next();
                    iterator.remove();

                    if(key.isAcceptable()){
                        handleAccept(key);
                    }else if(key.isReadable()){
                        handleRead(key);
                    }
                }
            }
        }catch(IOException e){
            out.println("IOException: " + e.getMessage());
        }
    }

    private static void handleAccept(SelectionKey key) throws IOException{
        ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
        SocketChannel socketChannel = serverChannel.accept();
        socketChannel.configureBlocking(false);
        socketChannel.register(key.selector(), SelectionKey.OP_READ);
    }

    private static void handleRead(SelectionKey key) throws IOException{
        SocketChannel socketChannel = (SocketChannel) key.channel();
        ByteBuffer buffer = ByteBuffer.allocate(256);
        int bytesRead = socketChannel.read(buffer);

        if(bytesRead == -1){
            socketChannel.close();
            return;
        }

        buffer.flip();
        String message = new String(buffer.array(), 0, buffer.limit());
        out.println("Received: " + message);

        List<String> commandLineList = parseCommandList(message);
        if(commandLineList == null || commandLineList.isEmpty()){
            out.println("commandLineList is empty!");
            return;
        }

        String command = commandLineList.get(0);
        String responseMessage = "";

        switch(command.toUpperCase()){
            case "PING":
                responseMessage = parsePing(commandLineList);
                break;
            case "ECHO":
                responseMessage = parseEcho(commandLineList);
                break;
            case "SET":
                responseMessage = parseSet(commandLineList);
                break;
            case "GET":
                responseMessage = parseGet(commandLineList);
                break;
            case "CONFIG":
                responseMessage = parseConfig(commandLineList);
                break;
            case "KEYS":
                responseMessage = parseKeys(commandLineList);
                break;
            default:
                responseMessage = "-ERR unknown command\r\n";
                break;
        }

        socketChannel.write(ByteBuffer.wrap(responseMessage.getBytes()));
    }


    private static String parseKeys(List<String> commandLineList) throws IOException{

        for(Map.Entry<String, String> entry : localMap.entrySet()){
            out.println("key: " + entry.getKey() + " value: " + entry.getValue());
        }

        String[] keys;
        keys = localMap.keySet().toArray(new String[0]);
        out.println(keys);
        String result = String.format("*%d\r\n", keys.length);
        for(String key : keys){
            result += "$" + key.length() + "\r\n" + key + "\r\n";
        }
        return result;
    }

    private static void readRDBFile() throws IOException{
        String dir = config.get("dir");
        String dbfilename = config.get("dbfilename");
        String filepath = dir + "/" + dbfilename;
        try(
                FileInputStream fis = new FileInputStream(filepath);
        ){

            //header section
            byte[] header = fis.readNBytes(9);
            out.println("header section: " + new String(header));

            int nextByte;
            while((nextByte = fis.read()) != -1){
                // metadata section
                if(nextByte == Integer.valueOf("FA", 16)){
                    out.println("in metadata section");
                    StringBuilder sb = new StringBuilder();
                    while((nextByte = fis.read()) != Integer.valueOf("FE", 16)){
                        // handler for metadata
                        sb.append((char) nextByte);
                    }

                    out.println("metadata String: " + sb);
                    //database section
                    out.println("start database section");


                    Integer index = fis.read();
                    out.println("the index of the database: " + index);


                    // fb section
                    while((nextByte = fis.read()) != Integer.valueOf("FC", 16) && nextByte != -1){
                        // handler for database  fb section
                        Integer hashTableSize = nextByte;
                        out.println("total hash table size: " + hashTableSize);
                        Integer expireHashtableSize = fis.read();
                        out.println("expire hash table size: " + expireHashtableSize);


                        if(hashTableSize == 0){
                            out.println("empty!");
                        }else{
                            StringBuilder kvsb = new StringBuilder();
                            String key = "";
                            String value = "";
                            Integer valueType = fis.read();
                            out.println("value type or encode: " + valueType);

                            int odd = 0;

                            while((nextByte = fis.read()) != Integer.valueOf("FF", 16)){

                                if(nextByte < 32 && odd % 2 == 0){
                                    key = kvsb.toString();
                                    kvsb.setLength(0);
                                    odd++;
                                    continue;
                                }
                                if(nextByte < 32 && odd % 2 == 1){
                                    value = kvsb.toString();
                                    kvsb.setLength(0);
                                    odd++;

                                    out.println("key-value: " + key + ":" + value);
                                    localMap.put(key, value);

                                    // skip one byte?
                                    if(odd > 0 && fis.read() == Integer.valueOf("FF", 16)){
                                        break;
                                    }
                                    continue;
                                }
                                kvsb.append((char) nextByte);
                            }


                            out.println("start end of file section");
                            byte[] checksum = fis.readNBytes(8);
                            BigInteger b1 = new BigInteger(1, checksum);
                            out.println("checksum: " + b1.toString(16));
                            break;
                        }
                    }

//                    while((nextByte = fis.read()) != Integer.valueOf("FD", 16)){
//                        // handler for expire time fc
//                    }
//
//                    while((nextByte = fis.read()) != Integer.valueOf("FF", 16)){
//                        // handler for expire time fd
//                    }

                }
            }

            out.println("end of file");
        }

    }

    private static String parsePing(List<String> commandLineList){
        return "+PONG\r\n";
    }

    private static String parseEcho(List<String> commandLineList){
        String message = commandLineList.get(1);
        return String.format("$%d\r\n%s\r\n", message.length(), message);
    }

    private static String parseSet(List<String> commandLineList){
        String key = commandLineList.get(1);
        String value = commandLineList.get(2);

        if(commandLineList.size() >= 4){
            String expireCommand = commandLineList.get(3);
            if("PX".equalsIgnoreCase(expireCommand)){
                long milSeconds = Long.parseLong(commandLineList.get(4));
                timedMap.put(key, new TimedValue(value, milSeconds));
            }
        }else{
            timedMap.put(key, new TimedValue(value, -1));
        }
        return "+OK\r\n";
    }

    private static String parseGet(List<String> commandLineList){
        String key = commandLineList.get(1);

        if(localMap.get(key) != null){
            return String.format("$%d\r\n%s\r\n", localMap.get(key).length(),
                    localMap.get(key));
        }

        TimedValue timedValue = timedMap.get(key);

        if(timedValue == null){
            return "$-1\r\n";
        }

        if(timedValue.getExpireTime() != -1 && timedValue.createLocalDateTime
                .plus(timedValue.getExpireTime(), ChronoUnit.MILLIS).isBefore(LocalDateTime.now())){
            timedMap.remove(key);
            return "$-1\r\n";
        }else{
            return String.format("$%d\r\n%s\r\n", timedValue.getValue().length(),
                    timedValue.getValue());
        }
    }


    private static String parseConfig(List<String> commandLineList){

        String parameterString = commandLineList.get(2);

        String result = String.format("*%d\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", 2,
                parameterString.length(),
                parameterString, config.get(parameterString).length(),
                config.get(parameterString));
        return result;
    }

    public static List<String> parseCommandList(String message){
        out.println("====parseCommandList====");
        List<String> resuList = new ArrayList<>();

        String[] lines = message.split("\r\n");
        for(String line : lines){
            out.println("====line====" + line);

            if(containCommandType(line)){
                out.println("====skip command====: " + line);
                continue;
            }
            resuList.add(line);
        }

        out.println("====out while====");
        return resuList;
    }

    static boolean containCommandType(String line){
        for(CommandType item : CommandType.values()){
            if(line.toLowerCase().contains(String.valueOf(item.value))){
                return true;
            }
        }
        return false;
    }

    static enum CommandType{
        SIMPLE_STRINGS('+'),
        SIMPLE_ERRORS('-'),
        INTEGERS(':'),
        BULK_STRINGS('$'),
        ARRAYS('*'),
        NULLS('_'),
        BOOLEANS('#'),
        DOUBLES(','),
        BIG_NUMBERS('('),
        BULK_ERRORS('!'),
        VERBATIM_STRINGS('='),
        MAPS('%'),
        ATTRIBUTES('`'),
        SETS('~'),
        PUSHES('>');

        private char value;

        CommandType(char value){
            this.value = value;
        }

        private char getValue(){
            return value;
        }

        public static CommandType getCommandTypeByValue(char value){
            for(CommandType v : CommandType.values()){
                if(value == v.getValue()){
                    return v;
                }
            }
            return null;
        }

    }

    static class TimedValue{

        private final String value;

        private final long expireTime;

        private final LocalDateTime createLocalDateTime;

        public TimedValue(String value, long expireTime){
            this.value = value;
            this.expireTime = expireTime;
            this.createLocalDateTime = LocalDateTime.now();
        }

        public String getValue(){
            return value;
        }

        public long getExpireTime(){
            return expireTime;
        }

        public LocalDateTime getCreateLocalDateTime(){
            return createLocalDateTime;
        }

        @Override
        public String toString(){
            return "TimedValue [value=" + value + ", expireTime=" + expireTime + ", createLocalDateTime="
                    + createLocalDateTime + "]";
        }

    }

}
