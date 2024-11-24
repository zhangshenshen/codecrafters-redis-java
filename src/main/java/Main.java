import static java.lang.System.out;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Main {

  private static final ConcurrentHashMap<String, TimedValue> timedMap = new ConcurrentHashMap<>();

  public static void main(String[] args) {
    try (Selector selector = Selector.open();
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {

      serverSocketChannel.bind(new InetSocketAddress(6379));
      serverSocketChannel.configureBlocking(false);
      serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);

      while (true) {
        selector.select(); // 阻塞直到有事件发生
        Set<SelectionKey> selectedKeys = selector.selectedKeys();
        Iterator<SelectionKey> iterator = selectedKeys.iterator();

        while (iterator.hasNext()) {
          SelectionKey key = iterator.next();
          iterator.remove();

          if (key.isAcceptable()) {
            handleAccept(key);
          } else if (key.isReadable()) {
            handleRead(key);
          }
        }
      }
    } catch (IOException e) {
      out.println("IOException: " + e.getMessage());
    }
  }

  private static void handleAccept(SelectionKey key) throws IOException {
    ServerSocketChannel serverChannel = (ServerSocketChannel) key.channel();
    SocketChannel socketChannel = serverChannel.accept();
    socketChannel.configureBlocking(false);
    socketChannel.register(key.selector(), SelectionKey.OP_READ);
  }

  private static void handleRead(SelectionKey key) throws IOException {
    SocketChannel socketChannel = (SocketChannel) key.channel();
    ByteBuffer buffer = ByteBuffer.allocate(256);
    int bytesRead = socketChannel.read(buffer);

    if (bytesRead == -1) {
      socketChannel.close();
      return;
    }

    buffer.flip();
    String message = new String(buffer.array(), 0, buffer.limit());
    out.println("Received: " + message);

    List<String> commandLineList = parseCommandList(message);
    if (commandLineList == null || commandLineList.isEmpty()) {
      out.println("commandLineList is empty!");
      return;
    }

    String command = commandLineList.get(0);
    String responseMessage = "";

    switch (command.toUpperCase()) {
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
      default:
        responseMessage = "-ERR unknown command\r\n";
        break;
    }

    socketChannel.write(ByteBuffer.wrap(responseMessage.getBytes()));
  }

  private static String parsePing(List<String> commandLineList) {
    return "+PONG\r\n";
  }

  private static String parseEcho(List<String> commandLineList) {
    String message = commandLineList.get(1);
    return String.format("$%d\r\n%s\r\n", message.length(), message);
  }

  private static String parseSet(List<String> commandLineList) {
    String key = commandLineList.get(1);
    String value = commandLineList.get(2);

    if (commandLineList.size() >= 4) {
      String expireCommand = commandLineList.get(3);
      if ("PX".equalsIgnoreCase(expireCommand)) {
        long milSeconds = Long.parseLong(commandLineList.get(4));
        timedMap.put(key, new TimedValue(value, milSeconds));
      }
    } else {
      timedMap.put(key, new TimedValue(value, -1));
    }
    return "+OK\r\n";
  }

  private static String parseGet(List<String> commandLineList) {
    String key = commandLineList.get(1);
    TimedValue timedValue = timedMap.get(key);

    if (timedValue == null) {
      return "$-1\r\n";
    }

    if (timedValue.getExpireTime() != -1 && timedValue.createLocalDateTime
        .plus(timedValue.getExpireTime(), ChronoUnit.MILLIS).isBefore(LocalDateTime.now())) {
      timedMap.remove(key);
      return "$-1\r\n";
    } else {
      return String.format("$%d\r\n%s\r\n", timedValue.getValue().length(),
          timedValue.getValue());
    }
  }

  private static String parseConfig(List<String> commandLineList) {
    String commandString = commandLineList.get(1);

    String parameterString = commandLineList.get(2);

    String result = "";
    switch (commandString.toUpperCase()) {
      case "GET":
        break;

      default:
        break;
    }
    switch (parameterString.toLowerCase()) {
      case "dir":
        result = "*2\r\n" +
            "$3\r\n" +
            "dir\r\n" +
            "$16\r\n" +
            "/tmp/redis-files\r\n";
        break;

      case "dbfilename":
        result = "*2\r\n" +
            "$3\r\n" +
            "dbfilename\r\n" +
            "$8\r\n" +
            "dump.rdb\r\n";
        break;
      default:
        break;
    }
    return result;
  }

  public static List<String> parseCommandList(String message) {
    out.println("====parseCommandList====");
    List<String> resuList = new ArrayList<>();

    String[] lines = message.split("\r\n");
    for (String line : lines) {
      out.println("====line====" + line);

      if (containCommandType(line)) {
        out.println("====skip command====: " + line);
        continue;
      }
      resuList.add(line);
    }

    out.println("====out while====");
    return resuList;
  }

  static boolean containCommandType(String line) {
    for (CommandType item : CommandType.values()) {
      if (line.toLowerCase().contains(String.valueOf(item.value))) {
        return true;
      }
    }
    return false;
  }

  static enum CommandType {
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

    CommandType(char value) {
      this.value = value;
    }

    private char getValue() {
      return value;
    }

    public static CommandType getCommandTypeByValue(char value) {
      for (CommandType v : CommandType.values()) {
        if (value == v.getValue()) {
          return v;
        }
      }
      return null;
    }

  }

  static class TimedValue {

    private final String value;

    private final long expireTime;

    private final LocalDateTime createLocalDateTime;

    public TimedValue(String value, long expireTime) {
      this.value = value;
      this.expireTime = expireTime;
      this.createLocalDateTime = LocalDateTime.now();
    }

    public String getValue() {
      return value;
    }

    public long getExpireTime() {
      return expireTime;
    }

    public LocalDateTime getCreateLocalDateTime() {
      return createLocalDateTime;
    }

    @Override
    public String toString() {
      return "TimedValue [value=" + value + ", expireTime=" + expireTime + ", createLocalDateTime="
          + createLocalDateTime + "]";
    }

  }

}
