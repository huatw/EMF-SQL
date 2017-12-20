import java.util.*;

public class Topo {
    private Map<Integer, Set<Integer>> dependencyMap;
    private HashSet<Integer> stack;
    private LinkedHashSet<Integer> done;

    private void loop(Integer key) throws Exception {
        Set<Integer> dependencies = dependencyMap.get(key);
        if (dependencies == null || dependencies.size() == 0) {
            done.add(key);
            return;
        }
        if (stack.contains(key)) {
            throw new Exception("TOPO SORT: cyclic depencencies.");
        }
        stack.add(key);
        for (int d: dependencies) {
            if (!done.contains(d)) {
                loop(d);
            }
        }
        stack.remove(key);
        done.add(key);
    }

    /**
     * topo sort
     * @return sorted topo order.
     */
    LinkedHashSet<Integer> sort(){
        stack = new HashSet<>();
        done = new LinkedHashSet<>();
        for (int key: dependencyMap.keySet()) {
            if (!done.contains(key)) {
                try {
                    loop(key);
                } catch (Exception e) {
                    System.out.println("TOPO SORT: cyclic dependencies.");
                    e.printStackTrace();
                }
            }
        }
        return done;
    }

    Topo(Map<Integer, Set<Integer>> dependencyMap) {
        this.dependencyMap = dependencyMap;
    }
}
