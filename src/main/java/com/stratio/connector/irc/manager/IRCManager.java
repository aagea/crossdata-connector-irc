package com.stratio.connector.irc.manager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jibble.pircbot.IrcException;
import org.jibble.pircbot.PircBot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class IRCManager {
    private static Logger LOG = LoggerFactory.getLogger(IRCManager.class);
    private static Map<String, List<Message>> channels = new HashMap<>();

    public CrossdataIRCBot bot;

    public IRCManager(String host, String name) {
        this.bot = new CrossdataIRCBot(name, host);
    }

    public void connect() throws Exception {
        try {
            bot.connect();
        } catch (IrcException e) {
            throw new Exception(e.getMessage(),e.getCause());
        }
    }

    public void joinChannel(String channel) {
        this.bot.joinChannel("#" + channel);
    }

    public void sendMessage(String channel, String message) {
        this.bot.send("#" + channel, message);
    }

    public List<Message> getMessagesFromChannel(String channel) {
        String realChannelName = "#" + channel;
        List<Message> result = new ArrayList<>();
        if (channels.containsKey(realChannelName)) {
            result = channels.get(realChannelName);
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        IRCManager manager = new IRCManager("127.0.0.1", "xdbot");
        manager.connect();
        manager.joinChannel("test");
        System.in.read();
        manager.sendMessage("test","This is a test.");
    }

    public void disconnect() {
        this.bot.disconnect();
    }

    class CrossdataIRCBot extends PircBot {
        private final String host;

        public CrossdataIRCBot(String name, String host) {
            this.setName(name);
            this.host = host;
            this.setVerbose(true);
        }

        public void connect() throws IrcException, IOException {
            this.connect(this.host);
            this.listChannels();

        }

        public void send(String channel, String message){
            this.sendMessage(channel,message);
            Message msg=new Message(this.getName(),this.host,channel,message);
            if(channels.containsKey(channel)){
                List<Message> messages=channels.get(channel);
                messages.add(msg);
            }
        }

        public void onMessage(String channel, String sender, String login, String hostname, String message) {
            LOG.info(channel + "-" + sender + "-" + login + "-" + hostname + "=>" + message);
            Message msg = new Message(sender, hostname, channel, message);
            if (!channels.containsKey(channel)) {
                channels.put(channel, new ArrayList<Message>());
            }
            List<Message> messages = channels.get(channel);
            messages.add(msg);
        }

        protected void onChannelInfo(String channel,
                int userCount,
                String topic) {
            LOG.info("DETECT CHANNEL: " + channel);
            this.joinChannel(channel);
        }
    }
}
