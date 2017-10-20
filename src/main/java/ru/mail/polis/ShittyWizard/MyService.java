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
        this.server = HttpServer.create(new InetSocketAddress(port), 0);
        this.server.createContext("/v0/status", this::StatusHandle);
        this.server.createContext("/v0/entity", this::EntityHandle);
        this.data = data;
    }

    @Override
    public void start() {
        this.server.start();
    }

    @Override
    public void stop() {
        this.server.stop(0);
    }

    private void StatusHandle(HttpExchange httpExchange) throws IOException {
        sendHttpResponse(httpExchange, 200, "OK");
    }

    private void EntityHandle(HttpExchange httpExchange) throws IOException {
        Map<String, String> params = MyService.queryToMap(httpExchange.getRequestURI().getQuery());
        if (!params.containsKey(KEY_ID)) {
            sendHttpResponse(httpExchange, 404, "Need ID");
            return;
        }
        if (params.get(KEY_ID).isEmpty()) {
            sendHttpResponse(httpExchange, 400, "Empty ID");
            return;
        }
        File file = new File(data.getAbsolutePath() + params.get(KEY_ID));

        if (httpExchange.getRequestMethod().equalsIgnoreCase("GET")) {
            if (!file.exists()) {
                sendHttpResponse(httpExchange, 404, "Not found");
                return;
            }
            sendHttpResponse(httpExchange, 200, file);

        } else if (httpExchange.getRequestMethod().equalsIgnoreCase("PUT")) {
            if (!file.exists()) file.createNewFile();
            copyRequestBodyToFile(httpExchange, file);
            sendHttpResponse(httpExchange, 201, "Created");

        } else if (httpExchange.getRequestMethod().equalsIgnoreCase("DELETE")) {
            if (!file.exists() || file.delete()) {
                sendHttpResponse(httpExchange, 202, "Accepted");
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
        BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(new FileOutputStream(file));
        InputStream inputStream = httpExchange.getRequestBody();
        for (int n = inputStream.read(buffer); n > 0; n = inputStream.read(buffer))
            bufferedOutputStream.write(buffer);
        bufferedOutputStream.close();
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
