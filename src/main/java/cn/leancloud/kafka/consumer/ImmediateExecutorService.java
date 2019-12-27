package cn.leancloud.kafka.consumer;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link java.util.concurrent.ExecutorService} which run all the tasks in the thread which submit the task.
 * <p>
 * This is an shared {@link java.util.concurrent.ExecutorService} so it can not be shutdown.
 */
final class ImmediateExecutorService extends AbstractExecutorService {
    static final ImmediateExecutorService INSTANCE = new ImmediateExecutorService();

    private ImmediateExecutorService() {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return true;
    }

    @Override
    public void execute(Runnable command) {
        if (command == null) {
            throw new NullPointerException("command");
        } else {
            command.run();
        }
    }
}
