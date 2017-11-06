package ru.mail.polis.ShittyWizard;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ru.mail.polis.KVService;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.Set;

public class NodeService implements KVService {

    private HttpServer server;
    private File data;
    private Set<String> topology;
    private String delimiter;

    public NodeService(int port, File data, Set<String> topology) throws IOException {
        this.data = data;
        this.topology = topology;
        server = HttpServer.create();
        server.bind(new InetSocketAddress(port), 1);
        server.createContext("/v0/entity", this::EntityHandle);
        server.createContext("/v0/status", this::StatusHandle);
        if (data.getAbsolutePath().contains("/")) {
            delimiter = "/";
        } else {
            delimiter = "\\";
        }
        int i = 0;
        for (String host : HostManager.hosts) {
            if (port == Integer.valueOf(host.split(":")[2])) {
                BufferedOutputStream bos = new BufferedOutputStream(
                        new FileOutputStream(
                                new File(data.getAbsolutePath() + delimiter +
                                        HostManager.queries.get(i)
                                                .split("&")[0]
                                                .split("=")[1])
                        )
                );
                bos.write(HostManager.bodies.get(i));
                bos.close();
                HostManager.queries.remove(i);
                HostManager.hosts.remove(i);
                HostManager.bodies.remove(i);
                break;
            } else {
                i++;
            }
        }
    }

    @Override
    public void start() {
        server.start();
    }

    @Override
    public void stop() {
        server.stop(1);
    }

    private void StatusHandle(HttpExchange httpExchange) throws IOException {
        sendHttpResponse(httpExchange, 200, "OK");
    }

    private void EntityHandle(HttpExchange httpExchange) throws IOException {
        boolean master = false;
        String requestMethod = httpExchange.getRequestMethod();
        String query = httpExchange.getRequestURI().getQuery();
        String id;
        String replicas;

        int ack = -1;
        int from = -1;
        if (!httpExchange.getRequestHeaders().containsKey("Replication")) {
            master = true;
        }
        if (query.contains("&")) {
            String[] parameters = query.split("&");
            id = parameters[0];
            replicas = parameters[1];
            if (id.split("=").length == 1) {
                sendHttpResponse(httpExchange, 400, "Empty ID");
                return;
            }
            id = id.split("=")[1];
            if (replicas.split("=").length == 1) {
                sendHttpResponse(httpExchange, 400, "Empty replicas");
                return;
            }
            replicas = replicas.split("=")[1];
            ack = Integer.valueOf(replicas.split("/")[0]);
            from = Integer.valueOf(replicas.split("/")[1]);
            if (ack > from || ack == 0 || from == 0) {
                sendHttpResponse(httpExchange, 400, "Invalid parameters");
                return;
            }
        } else {
            if (query.split("=").length == 1) {
                sendHttpResponse(httpExchange, 400, "Empty ID");
                return;
            }
            id = query.split("=")[1];
        }
        if (ack == -1 || from == -1) {
            ack = topology.size() / 2 + 1;
            from = topology.size();
        }
        File file = new File(data.getAbsolutePath() + delimiter + id);
        if (requestMethod.equalsIgnoreCase("GET")) {
            if (master) {
                ReplicationManager replicationManager = new ReplicationManager(topology, ack, from, query, "GET", "http:/" + httpExchange.getLocalAddress().toString(), null);
                int status = replicationManager.replication();
                if (status == 0) {
                    if (!file.exists()) {
                        sendHttpResponse(httpExchange, 404, "Not found");
                        return;
                    }
                    sendHttpResponse(httpExchange, 200, file);
                } else if (status == -1) {
                    sendHttpResponse(httpExchange, 504, "Not Enough Replicas");
                } else {
                    sendHttpResponse(httpExchange, 404, "Not found");
                }
            } else {
                if (!file.exists()) {
                    sendHttpResponse(httpExchange, 404, "Not found");
                    return;
                }
                sendHttpResponse(httpExchange, 200, "OK");
            }
        } else if (requestMethod.equalsIgnoreCase("PUT")) {
            if (!file.exists()) {
                file.createNewFile();
            }
            byte[] buffer = new byte[1024];
            InputStream is = httpExchange.getRequestBody();
            BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file));
            for (int n = is.read(buffer); n > 0; n = is.read(buffer)) {
                bos.write(buffer);
            }
            bos.close();
            if (master) {
                ReplicationManager replicationManager = new ReplicationManager(topology, ack, from, query, "PUT", "http:/" + httpExchange.getLocalAddress().toString(), buffer);
                if (replicationManager.replication() == 0) {
                    sendHttpResponse(httpExchange, 201, "Created");
                } else {
                    sendHttpResponse(httpExchange, 504, "Not Enough Replicas");
                }
            } else {
                sendHttpResponse(httpExchange, 201, "Created");
            }
        } else if (requestMethod.equalsIgnoreCase("DELETE")) {
            if (!file.exists() || file.delete()) {
                if (master) {
                    ReplicationManager replicationManager = new ReplicationManager(topology, ack, from, query, "DELETE", "http:/" + httpExchange.getLocalAddress().toString(), null);
                    if (replicationManager.replication() == 0) {
                        sendHttpResponse(httpExchange, 202, "Accepted");
                    } else {
                        sendHttpResponse(httpExchange, 504, "Not Enough Replicas");
                    }
                } else {
                    sendHttpResponse(httpExchange, 202, "Accepted");
                }
            } else {
                throw new IOException();
            }
        } else {
            throw new IOException();
        }
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
}
