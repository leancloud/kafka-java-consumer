package cn.leancloud.kafka.consumer;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ImmediateExecutorServiceTest {

    @Test
    public void testExecuteNullRunnable() {
        assertThatThrownBy(() -> ImmediateExecutorService.INSTANCE.execute(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("command");
    }

    @Test
    public void testExecuteRunnable() {
        long expectThreadId = Thread.currentThread().getId();
        ImmediateExecutorService.INSTANCE.execute(() -> {
                    assert Thread.currentThread().getId() == expectThreadId;
                }
        );
    }

    @Test
    public void testShutdown() {
        ImmediateExecutorService.INSTANCE.shutdown();
        assertThat(ImmediateExecutorService.INSTANCE.isShutdown()).isFalse();
        assertThat(ImmediateExecutorService.INSTANCE.isTerminated()).isFalse();
    }

    @Test
    public void testShutdownNow() {
        assertThat(ImmediateExecutorService.INSTANCE.shutdownNow()).isEmpty();
        assertThat(ImmediateExecutorService.INSTANCE.isShutdown()).isFalse();
        assertThat(ImmediateExecutorService.INSTANCE.isTerminated()).isFalse();
    }

    @Test
    public void testAwaitTermination() throws Exception {
        assertThat(ImmediateExecutorService.INSTANCE.awaitTermination(1, TimeUnit.DAYS)).isTrue();
        assertThat(ImmediateExecutorService.INSTANCE.isShutdown()).isFalse();
        assertThat(ImmediateExecutorService.INSTANCE.isTerminated()).isFalse();
    }
}