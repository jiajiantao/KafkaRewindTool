package Models;

/**
 * Created by rishi on 30/10/15.
 */
public class Message {
    public Message(){}
    public Message(String time){
        this.time = time;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String time;
}
