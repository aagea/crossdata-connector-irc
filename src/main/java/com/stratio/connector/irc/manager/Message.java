package com.stratio.connector.irc.manager;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Message {
    private final String timestamp;
    private final String user;
    private final String host;
    private final String channel;
    private final String message;

    public Message(String user, String host, String channel, String message) {
        SimpleDateFormat dateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        this.timestamp =  dateFormat.format(new Date());
        this.user = user;
        this.host = host;
        this.channel = channel;
        this.message = message;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getUser() {
        return user;
    }

    public String getHost() {
        return host;
    }

    public String getChannel() {
        return channel;
    }

    public String getMessage() {
        return message;
    }
}
