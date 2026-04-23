package company.vk.edu.distrib.compute.arseniy90.stats;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

public class StatisticsAggregator {
    private final Set<String> localKeys = ConcurrentHashMap.newKeySet();
    private final LongAdder totalReads = new LongAdder();
    private final LongAdder totalWrites = new LongAdder();

    public void trackRead() { 
        totalReads.increment(); 
    }

    public void trackWrite(String id) { 
        totalWrites.increment(); 
        localKeys.add(id); 
    }

    public void trackDelete(String id) {
        totalWrites.increment(); 
        localKeys.remove(id);
    }

    public String getAccessJson() {
        long reads = totalReads.sum();
        long writes = totalWrites.sum();

        return String.format(
            "{\"reads\": %d, \"writes\": %d, \"total_access\": %d}",
            reads, 
            writes, 
            reads + writes
        );
    }

    public String getKeysJson() {
        return String.format("{\"keys\": %d}", localKeys.size());
    }
}
