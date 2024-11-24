import static java.lang.System.out;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
    Socket clientSocket = null;
    int port = 6379;

    try {
      serverSocket = new ServerSocket(port);
      // Since the tester restarts your program quite often, setting SO_REUSEADDR
      // ensures that we don't run into 'Address already in use' errors
      serverSocket.setReuseAddress(true);
      // Wait for connection from client.

      while (true) {
        clientSocket = serverSocket.accept();
        new Thread(new ClientHandler(clientSocket)).start();
      }
    } catch (IOException e) {
      System.out.println("IOException: " + e.getMessage());
    } finally {
      try {
        if (clientSocket != null) {
          clientSocket.close();
        }
      } catch (IOException e) {
        System.out.println("IOException: " + e.getMessage());
      }
    }
  }

  private static class ClientHandler implements Runnable {
    private final Socket clientSocket;

    public ClientHandler(Socket clientSocket) {
      this.clientSocket = clientSocket;
    }

    @Override
    public void run() {
      System.out.println("====into run====");
      try {
        List<String> commandLineList = parseCommandList(clientSocket.getInputStream());
        if (commandLineList == null || commandLineList.isEmpty()) {
          System.out.println("commandLineList is empty!");
          return;
        }
        String command = commandLineList.get(0);
        OutputStream out = clientSocket.getOutputStream();

        System.out.println("first command : " + command);

        switch (command.toUpperCase()) {
          case "PING":
            parsePing(commandLineList, out);
            break;

          case "ECHO":
            parseEcho(commandLineList, out);
            break;

          case "SET":
            parseSet(commandLineList, out);
            break;

          case "GET":
            parseGet(commandLineList, out);
            break;

          default:
            break;
        }

      } catch (Exception e) {
        System.out.println("Exception: " + e.getMessage());
      }
    }
  }

  private static void parsePing(List<String> commandLineList, OutputStream out) throws IOException {
    out.write("+PONG\r\n".getBytes());
  }

  private static void parseEcho(List<String> commandLineList, OutputStream out) throws IOException {
    String message = commandLineList.get(1);
    out.write(String.format("$%d\r\n%s\r\n", message.length(), message).getBytes());
  }

  private static void parseSet(List<String> commandLineList, OutputStream out) throws IOException {

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
    out.write("+OK\r\n".getBytes());
  }

  private static void parseGet(List<String> commandLineList, OutputStream out) throws IOException {
    String key = commandLineList.get(1);
    TimedValue timedValue = timedMap.get(key);

    if (timedValue.getExpireTime() != -1 && timedValue.createLocalDateTime
        .plus(timedValue.getExpireTime(), ChronoUnit.MILLIS).isBefore(LocalDateTime.now())) {
      out.write("$-1\r\n".getBytes());
    } else {
      out.write(String.format("$%d\r\n%s\r\n", timedValue.getValue().length(),
          timedValue.getValue()).getBytes());
    }
  }

  /**
   * parse out all command line at once;
   * 
   * @param inputStream
   * @return
   */
  public static List<String> parseCommandList(InputStream inputStream) {
    System.out.println("====parseCommandList====");
    List<String> resuList = new ArrayList<>();

    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {

      String line = "";
      while ((line = br.readLine()) != null) {
        resuList.add(line);
      }
    } catch (Exception e) {
      System.out.println("Exception message: " + e.getMessage());
    }
    return resuList;
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
