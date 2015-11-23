package hzindexissue;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.Member;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.query.Predicates;
import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class HzIndexIssueTest {

    @Test
    public void test() throws InterruptedException {
        HazelcastInstance node1 = createNode(1, true);
        HazelcastInstance node2 = createNode(2, true);

        node1.getMap("book").loadAll(true);
        Thread.sleep(10000);

        Set<Object> foundByPredicate = new TreeSet<>(node1.getMap("book").localKeySet(Predicates.and(
                Predicates.in("author", "0", "1", "2", "3", "4", "5", "6"),
                Predicates.between("year", 1990, 2000)
        )));

        Map<Member,Set<Object>> foundByPredicateByMember = foundByPredicate.stream()
                .collect(Collectors.groupingBy(
                        key -> node1.getPartitionService().getPartition(key).getOwner(),
                        Collectors.toCollection(TreeSet::new)));

        Assert.assertEquals(mapValuesSizesToString(foundByPredicateByMember), 1, foundByPredicateByMember.size());
        Assert.assertTrue(foundByPredicateByMember.keySet().iterator().next().localMember());

    }

    private String mapValuesSizesToString(Map<?, ? extends Collection<?>> map) {
        return map.entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue().size()))
                .toString();
    }

    private static HazelcastInstance createNode(int n, boolean indexAuthors) {
        Config config = new Config("library"+n);
        config.getGroupConfig().setName("books").setPassword("123123");
        if (indexAuthors) {
            config.getMapConfig("book").addMapIndexConfig(new MapIndexConfig("author", false));
            config.getMapConfig("book").addMapIndexConfig(new MapIndexConfig("year", true));
        }
        config.getMapConfig("book").setMapStoreConfig(new MapStoreConfig().setImplementation(new BookMapLoader(n)));
        config.getMapConfig("book").setBackupCount(0).setAsyncBackupCount(1);
        config.getNetworkConfig().addOutboundPortDefinition("45000-45100").setPort(45000 + n).setPortAutoIncrement(true);
        HazelcastInstance hazelcastInstance = HazelcastInstanceFactory.newHazelcastInstance(config);
        return hazelcastInstance;
    }

    public static class Book implements Serializable {
        private long bookdId;
        private String title;
        private String author;
        private int year;

        private Book() {
        }

        public Book(long bookdId, String title, String author, int year) {
            this.bookdId = bookdId;
            this.title = title;
            this.author = author;
            this.year = year;
        }

        public long getBookdId() {
            return bookdId;
        }

        public String getTitle() {
            return title;
        }

        public String getAuthor() {
            return author;
        }

        public int getYear() {
            return year;
        }
    }

    private static class BookMapLoader implements MapLoader<Integer,Book> {
        private int nodeId;

        public BookMapLoader(int nodeId) {
            this.nodeId = nodeId;
        }

        @Override
        public Book load(Integer key) {
            return loadAll(Collections.singleton(key)).get(key);
        }

        @Override
        public Map<Integer, Book> loadAll(Collection<Integer> keys) {
            System.out.println("node "+nodeId+": "+keys);
            Map<Integer, Book> map = new TreeMap<>();
            for (int key : keys) {
                map.put(key, new Book(key, String.valueOf(key), String.valueOf(key % 7), 1800 + key % 200));
            }
            return map;
        }

        @Override
        public Iterable<Integer> loadAllKeys() {
            return IntStream.range(0, 2000).mapToObj(Integer::valueOf).collect(Collectors.toCollection(TreeSet::new));
        }
    }
}
