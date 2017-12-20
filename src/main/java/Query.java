import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Query {
    String dbName;
    String className;

    HashMap<String, Class> typeMap = new HashMap<>();
    String tableName;
    String where;

    List<String> projections = new ArrayList<>();
    List<String> gAttrs = new ArrayList<>();
    HashMap<Integer, List<String>> aggFns = new HashMap<>();
    HashMap<Integer, String> suchthats = new HashMap<>();
    String having;
    LinkedList<Integer> topoOrder = new LinkedList<>();

    /**
     * generate topo dependencies.
     */
    void topoSort() {
        Map<Integer, Set<Integer>> dependencies = new HashMap<>();

        for (int i: suchthats.keySet()) {
            String suchthat = suchthats.get(i);
            Set<Integer> set = new HashSet<>();
            Pattern rInt = Pattern.compile("\\d+");
            Matcher mInt = rInt.matcher(suchthat);
            while (mInt.find()) {
                Integer d = Integer.valueOf(mInt.group());
                if (d != i) set.add(d);
            }
            dependencies.put(i, set);
        }

        LinkedHashSet<Integer> order = new Topo(dependencies).sort();
        topoOrder.addAll(order);
    }
}