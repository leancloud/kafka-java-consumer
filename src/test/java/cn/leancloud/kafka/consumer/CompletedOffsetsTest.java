package cn.leancloud.kafka.consumer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class CompletedOffsetsTest {
    private CompletedOffsets offsets;
    @Before
    public void setUp() throws Exception {
        offsets = new CompletedOffsets(100);
    }

    @Test
    public void testNoRecords() {
        assertThat(offsets.hasOffsetToCommit()).isFalse();
        assertThat(offsets.hasOffsetToCommit()).isFalse();
    }

    @Test
    public void testAddContinuousRecords() {
        offsets.addCompleteOffset(101);
        offsets.addCompleteOffset(102);
        offsets.addCompleteOffset(103);

        assertThat(offsets.hasOffsetToCommit()).isTrue();
        assertThat(offsets.getOffsetToCommit()).isEqualTo(new OffsetAndMetadata(104));
    }

    @Test
    public void testNonContinuousRecords() {
        offsets.addCompleteOffset(101);
        offsets.addCompleteOffset(103);
        offsets.addCompleteOffset(104);
        offsets.addCompleteOffset(105);

        assertThat(offsets.hasOffsetToCommit()).isTrue();
        assertThat(offsets.getOffsetToCommit()).isEqualTo(new OffsetAndMetadata(102));
    }

    @Test
    public void testNonContinuousRecords2() {
        offsets.addCompleteOffset(101);
        offsets.addCompleteOffset(103);
        offsets.addCompleteOffset(104);
        offsets.addCompleteOffset(105);

        offsets.addCompleteOffset(102);

        assertThat(offsets.hasOffsetToCommit()).isTrue();
        assertThat(offsets.getOffsetToCommit()).isEqualTo(new OffsetAndMetadata(106));
    }

    @Test
    public void testUpdateCommittedOffset() {
        offsets.addCompleteOffset(101);
        offsets.addCompleteOffset(102);
        offsets.addCompleteOffset(103);

        offsets.updateCommittedOffset(103);
        assertThat(offsets.hasOffsetToCommit()).isFalse();
        assertThat(offsets.getOffsetToCommit()).isEqualTo(new OffsetAndMetadata(104));
    }
}