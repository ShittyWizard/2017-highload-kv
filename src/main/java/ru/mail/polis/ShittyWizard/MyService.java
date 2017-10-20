package ru.mail.polis.ShittyWizard;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

public class MyService implements KVService {

    private static final String KEY_ID = "id";

    private HttpServer server;
    private File data;

    public MyService(int port, File data) throws IOException {
        this.data = data;
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/v0/status", this::StatusHandle);
        this.server.createContext("/v0/entity", this::EntityHandle);
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(0);
    }

    private void StatusHandle(HttpExchange http) throws IOException {
        sendHttpResponse(http, 200, "OK");
    }

    private void EntityHandle(HttpExchange http) throws IOException {
        Map<String, String> params = MyService.queryToMap(http.getRequestURI().getQuery());
        if (!params.containsKey(KEY_ID)) {
            sendHttpResponse(http, 404, "Need ID");
            return;
        }
        if (params.get(KEY_ID).isEmpty()) {
            sendHttpResponse(http, 400, "Empty ID");
            return;
        }
        File file = new File(data.getAbsolutePath() + params.get(KEY_ID));

        if (http.getRequestMethod().equalsIgnoreCase("GET")) {
            if (!file.exists()) {
                sendHttpResponse(http, 404, "Not found");
                return;
            }
            sendHttpResponse(http, 200, file);

        } else if (http.getRequestMethod().equalsIgnoreCase("PUT")) {
            if (!file.exists()) file.createNewFile();
            copyRequestBodyToFile(http, file);
            sendHttpResponse(http, 201, "Created");

        } else if (http.getRequestMethod().equalsIgnoreCase("DELETE")) {
            if (!file.exists() || file.delete()) {
                sendHttpResponse(http, 202, "Accepted");
            } else
                throw new IOException();

        } else
            throw new IOException();
    }

    private void sendHttpResponse(HttpExchange httpExchange, int code, byte[] data) throws IOException {
        httpExchange.sendResponseHeaders(code, data.length);
        httpExchange.getResponseBody().write(data);
        httpExchange.getResponseBody().close();
    }

    private void sendHttpResponse(HttpExchange httpExchange, int code, String message) throws IOException {
        sendHttpResponse(httpExchange, code, message.getBytes());
    }

    private void sendHttpResponse(HttpExchange httpExchange, int code, File file) throws IOException {
        httpExchange.sendResponseHeaders(code, file.length());
        OutputStream outputStream = httpExchange.getResponseBody();
        Files.copy(file.toPath(), outputStream);
        outputStream.close();
    }

    private void copyRequestBodyToFile(HttpExchange httpExchange, File file) throws IOException {
        byte[] buffer = new byte[1024];
        BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
        InputStream is = httpExchange.getRequestBody();
        for (int n = is.read(buffer); n > 0; n = is.read(buffer))
            bos.write(buffer);
        bos.close();
    }

    private static Map<String, String> queryToMap(String query) {
        if (query == null)
            return new HashMap<>();

        Map<String, String> result = new HashMap<>();
        for (String param : query.split("&")) {
            String pair[] = param.split("=");
            if (pair.length > 1) {
                result.put(pair[0], pair[1]);
            } else {
                result.put(pair[0], "");
            }
        }
        return result;
    }
}
