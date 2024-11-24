import static java.lang.System.out;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class Main {

  private static final ConcurrentHashMap<String, TimedValue> timedMap = new ConcurrentHashMap<>();

  public static void main(String[] args) {
    // You can use print statements as follows for debugging, they'll be visible
    // when running tests.
    out.println("Logs from your program will appear here!");

    // Uncomment this block to pass the first stage
    ServerSocket serverSocket = null;
    int port = 6379;

    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.
      while (true) {
        Socket clientSocket = serverSocket.accept();
        new Thread(new ClientHandler(clientSocket)).start();
      }
    } catch (IOException e) {
      out.println("IOException: " + e.getMessage());
    }
  }

  private static class ClientHandler implements Runnable {
    private final Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
      this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
      out.println("====into run====");
      try {
        List<String> commandLineList = parseCommandList(clientSocket.getInputStream());
        if (commandLineList == null || commandLineList.isEmpty()) {
          out.println("commandLineList is empty!");
          return;
        }
        out.println("====out parseCommandList====");
        out.println("commandList: " + commandLineList);
        String command = commandLineList.get(0);
        out.println("first command : " + command);

        String responseMessage = "";
        switch (command.toUpperCase()) {
          case "PING":
            responseMessage = parsePing(commandLineList);
            out.println("====case PING==== responseMessage: " + responseMessage);
            break;

          case "ECHO":
            responseMessage = parseEcho(commandLineList);
            out.println("====case ECHO==== responseMessage: " + responseMessage);
            break;

          case "SET":
            responseMessage = parseSet(commandLineList);
            out.println("====case SET==== responseMessage: " + responseMessage);

            break;

          case "GET":
            responseMessage = parseGet(commandLineList);
            out.println("====case GET==== responseMessage: " + responseMessage);

            break;

          default:
            out.println("====case DEFAULT==== responseMessage: " + responseMessage);

            break;
        }
        out.println("==== out switch====");
        clientSocket.getOutputStream().write(responseMessage.getBytes());
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private static String parsePing(List<String> commandLineList) {
    out.println("====into parse PING====");
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

    if (timedValue.getExpireTime() != -1 && timedValue.createLocalDateTime
        .plus(timedValue.getExpireTime(), ChronoUnit.MILLIS).isBefore(LocalDateTime.now())) {
      return "$-1\\r\\n";
    } else {
      return String.format("$%d\r\n%s\r\n", timedValue.getValue().length(),
          timedValue.getValue());
    }
  }

  /**
   * parse out all command line at once;
   * 
   * @param inputStream
   * @return
   */
  public static List<String> parseCommandList(InputStream inputStream) {
    out.println("====parseCommandList====");
    List<String> resuList = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while (br.ready() && (line = br.readLine()) != null) {
        out.println("====line====" + line);

        if (containCommandType(line)) {
          out.println("====skip command====: " + line);
          continue;
        }
        resuList.add(line);
      }
      out.println("====out while====");
    } catch (Exception e) {
      out.println("Exception message: " + e.getMessage());
    }
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

    private String value;

    private long expireTime;

    private LocalDateTime createLocalDateTime;

    public TimedValue(String value, long expireTime) {
      this.value = value;
      this.expireTime = expireTime;
      this.createLocalDateTime = LocalDateTime.now();
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }

    public long getExpireTime() {
      return expireTime;
    }

    public void setExpireTime(long expireTime) {
      this.expireTime = expireTime;
    }

    public LocalDateTime getCreateLocalDateTime() {
      return createLocalDateTime;
    }

    public void setCreateLocalDateTime(LocalDateTime createLocalDateTime) {
      this.createLocalDateTime = createLocalDateTime;
    }

    @Override
    public String toString() {
      return "TimedValue [value=" + value + ", expireTime=" + expireTime + ", createLocalDateTime="
          + createLocalDateTime + "]";
    }

  }

}
