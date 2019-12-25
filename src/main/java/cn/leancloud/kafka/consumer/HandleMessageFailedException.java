package cn.leancloud.kafka.consumer;

public class HandleMessageFailedException extends RuntimeException {
    public HandleMessageFailedException() {
        super();
    }

    public HandleMessageFailedException(String message) {
        super(message);
    }

    public HandleMessageFailedException(Throwable throwable) {
        super(throwable);
    }

    public HandleMessageFailedException(String message, Throwable throwable) {
        super(message, throwable);
    }
}

