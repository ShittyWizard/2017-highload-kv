package ru.mail.polis.ShittyWizard;

import java.util.ArrayList;

public class HostManager {
    static ArrayList<String> hosts = new ArrayList<>();
    static ArrayList<String> queries = new ArrayList<>();
    static ArrayList<byte[]> bodies = new ArrayList<>();

    public static void addHost(String host, String query, byte[] body) {
        hosts.add(host);
        queries.add(query);
        bodies.add(body);
    }

}
