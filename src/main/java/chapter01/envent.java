package chapter01;

import java.sql.Timestamp;

public class envent {
    public String user;
    public String url;
    public Long timestamp;

    public envent() {
    }

    public envent(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "envent{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp)  +
                '}';
    }
}
