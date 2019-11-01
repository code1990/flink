package part09kafkaevent;

/**
 * @program: flinkuser
 * @Date: 2019-11-02 4:32
 * @Author: code1990
 * @Description:
 */
public class KafkaEvent {

    private final static String splitword = "##";
    private String word;
    private int frequency;
    private long timestamp;

    public static String getSplitword() {
        return splitword;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getFrequency() {
        return frequency;
    }

    public void setFrequency(int frequency) {
        this.frequency = frequency;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public KafkaEvent() {}

    public KafkaEvent(String word, int frequency, long timestamp) {
        this.word = word;
        this.frequency = frequency;
        this.timestamp = timestamp;
    }

    public static KafkaEvent fromString(String eventStr) {
        String[] split = eventStr.split(splitword);
        return new KafkaEvent(split[0], Integer.valueOf(split[1]), Long.valueOf(split[2]));
    }

    @Override
    public String toString() {
        return word +splitword + frequency + splitword + timestamp;
    }
}
