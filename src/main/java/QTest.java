import java.util.*;

public class QTest {
    /**
     * db config.
     * @param q
     */
    static void loadPublic(Query q) {
        q.dbName = "jdbc:postgresql://localhost:5432/emfquery";
        q.className = "Test";

        q.typeMap.put("cust", String.class);
        q.typeMap.put("month", int.class);
        q.typeMap.put("quant", int.class);
        q.typeMap.put("prod", String.class);
        q.typeMap.put("state", String.class);
        q.typeMap.put("avg_quant_0", double.class);
        q.typeMap.put("avg_quant_1", double.class);
        q.typeMap.put("avg_quant_2", double.class);
        q.typeMap.put("avg_quant_3", double.class);
        q.typeMap.put("sum_quant_0", int.class);
        q.typeMap.put("sum_quant_1", int.class);
        q.typeMap.put("sum_quant_2", int.class);
        q.typeMap.put("sum_quant_3", int.class);
        q.typeMap.put("cnt_quant_0", int.class);
        q.typeMap.put("cnt_quant_1", int.class);
        q.typeMap.put("cnt_quant_2", int.class);
        q.typeMap.put("cnt_quant_3", int.class);
        q.typeMap.put("max_quant_0", int.class);
        q.typeMap.put("max_quant_1", int.class);
        q.typeMap.put("max_quant_2", int.class);
        q.typeMap.put("max_quant_3", int.class);
    }

    /**
     ESQL-Q1:------------------------------------------------------
     SELECT cust, month, avg(x.quant), avg(quant), avg(y.quant)
     FROM sales
     WHERE year=1990
     GROUP BY cust, month; x, y
     SUCH THAT x.cust=cust and x.month < month,
               y.cust=cust and y.month > month
     HAVING avg(x.quant) < avg(y.quant)

     SQL-Q1:------------------------------------------------------
     drop view IF EXISTS V1 CASCADE;
     drop view IF EXISTS V2 CASCADE;
     drop view IF EXISTS V3 CASCADE;

     create view V1 as (
     select cust, month, avg(quant)
     from sales
     where year=1990
     group by cust, month
     );

     create view V2 as (
     select s1.cust, s1.month, avg(s2.quant)
     from sales s1, sales s2
     where s1.year=1990 and s2.year=1990 and s1.cust=s2.cust and s1.month > s2.month
     group by s1.cust, s1.month
     );

     create view V3 as (
     select s1.cust, s1.month, avg(s2.quant)
     from sales s1, sales s2
     where s1.year=1990 and s2.year=1990 and s1.cust=s2.cust and s1.month < s2.month
     group by s1.cust, s1.month
     );

     select V1.cust, V1.month, V2.avg as prev_avg, V1.avg, V3.avg as next_avg
     from V1, V2, V3
     where V1.cust = V2.cust and V1.cust = V3.cust
     and V1.month=V2.month and V1.month=V3.month
     and V2.avg < V3.avg;

     RESULT:-------------------------------------------------------------------------
     cust  | month |       prev_avg        |          avg          |       next_avg
     -------+-------+-----------------------+-----------------------+-----------------------
     Bloom |     7 | 1555.7142857142857143 |  656.5000000000000000 | 2602.8750000000000000
     Helen |     2 |  728.0000000000000000 | 2513.0000000000000000 | 1233.2000000000000000
     Bloom |     8 | 1355.8888888888888889 | 3798.0000000000000000 | 2432.1428571428571429
     Bloom |     9 | 1600.1000000000000000 | 1951.3333333333333333 | 2792.7500000000000000
     Knuth |     8 | 3692.6666666666666667 | 2063.0000000000000000 | 4672.0000000000000000
     Bloom |     4 | 1799.7500000000000000 | 1717.5000000000000000 | 2035.6363636363636364
     Bloom |     6 | 1772.3333333333333333 |  256.0000000000000000 | 2213.6000000000000000
     Helen |     5 | 1405.1666666666666667 |  936.0000000000000000 | 1556.6000000000000000
     Bloom |     2 |  596.0000000000000000 | 3003.5000000000000000 | 1986.6923076923076923
     (9 rows)

     Helen      5          1405.166667      936.000000       1556.600000
     Helen      2          728.000000       2513.000000      1233.200000
     Bloom      2          596.000000       3003.500000      1986.692308
     Bloom      8          1355.888889      3798.000000      2432.142857
     Bloom      7          1555.714286      656.500000       2602.875000
     Knuth      8          3692.666667      2063.000000      4672.000000
     Bloom      9          1600.100000      1951.333333      2792.750000
     Bloom      4          1799.750000      1717.500000      2035.636364
     Bloom      6          1772.333333      256.000000       2213.600000
     */
    static void loadQ1(Query q) {
        q.tableName = "sales";
        q.where = "year=1990";

        q.gAttrs.add("cust");
        q.gAttrs.add("month");

        q.projections.add("cust");
        q.projections.add("month");
        q.projections.add("avg_quant_1");
        q.projections.add("avg_quant_0");
        q.projections.add("avg_quant_2");

        List<String> aggFns0 = new ArrayList<>();
        List<String> aggFns1 = new ArrayList<>();
        List<String> aggFns2 = new ArrayList<>();
        aggFns0.add("sum_quant_0");
        aggFns0.add("cnt_quant_0");
        aggFns0.add("avg_quant_0");
        aggFns0.add("max_quant_0");
        aggFns1.add("sum_quant_1");
        aggFns1.add("cnt_quant_1");
        aggFns1.add("avg_quant_1");
        aggFns1.add("max_quant_1");
        aggFns2.add("sum_quant_2");
        aggFns2.add("cnt_quant_2");
        aggFns2.add("avg_quant_2");
        aggFns2.add("max_quant_2");
        q.aggFns.put(0, aggFns0);
        q.aggFns.put(1, aggFns1);
        q.aggFns.put(2, aggFns2);

        q.suchthats.put(1, "cust_1 = cust_0 and month_1 < month_0");
        q.suchthats.put(2, "cust_2 = cust_0 and month_2 > month_0");

        q.having = "avg_quant_1 < avg_quant_2";
    }

    /**
     ESQL-Q2:------------------------------------------------------
     SELECT cust, avg(x.sale), avg(y.sale), avg(z.sale)
     FROM sales
     WHERE year=1990
     GROUP BY cust; x,y,z
     SUCH THAT x.cust=cust and x.state='NY',
               y.cust=cust and y.state='CT',
               z.cust=cust and z.state='NJ'

     SQL-Q2:------------------------------------------------------
     select x.cust, avg(x.quant), avg(y.quant), avg(z.quant)
     from sales x, sales y, sales z
     where x.year = 1990 and y.year = 1990 and z.year = 1990
       and x.cust = y.cust and x.cust = z.cust
       and x.state = 'NY' and y.state = 'CT' and z.state = 'NJ'
     group by x.cust;

     RESULT:-------------------------------------------------------------------------
     cust  |          avg          |          avg          |          avg
     -------+-----------------------+-----------------------+-----------------------
     Bloom | 1728.0000000000000000 | 2207.8000000000000000 | 2078.5000000000000000
     Emily | 1943.0000000000000000 | 1733.0000000000000000 | 2312.2500000000000000
     Helen |  938.7500000000000000 |  894.5000000000000000 | 1081.0000000000000000
     Knuth | 3445.0000000000000000 | 2063.0000000000000000 | 4454.5000000000000000
     (4 rows)

     Emily      1943.000000      1733.000000      2312.250000
     Knuth      3445.000000      2063.000000      4454.500000
     Bloom      1728.000000      2207.800000      2078.500000
     Helen      938.750000       894.500000       1081.000000
     */
    static void loadQ2(Query q) {
        q.tableName = "sales";
        q.where = "year=1990";

        q.gAttrs.add("cust");

        q.projections.add("cust");
        q.projections.add("avg_quant_1");
        q.projections.add("avg_quant_2");
        q.projections.add("avg_quant_3");

        List<String> aggFns1 = new ArrayList<>();
        List<String> aggFns2 = new ArrayList<>();
        List<String> aggFns3 = new ArrayList<>();
        aggFns1.add("sum_quant_1");
        aggFns1.add("cnt_quant_1");
        aggFns1.add("avg_quant_1");
        aggFns2.add("sum_quant_2");
        aggFns2.add("cnt_quant_2");
        aggFns2.add("avg_quant_2");
        aggFns3.add("sum_quant_3");
        aggFns3.add("cnt_quant_3");
        aggFns3.add("avg_quant_3");
        q.aggFns.put(1, aggFns1);
        q.aggFns.put(2, aggFns2);
        q.aggFns.put(3, aggFns3);

        q.suchthats.put(1, "cust_1 = cust_0 and state_1 = \"NY\"");
        q.suchthats.put(2, "cust_2 = cust_0 and state_2 = \"CT\"");
        q.suchthats.put(3, "cust_3 = cust_0 and state_3 = \"NJ\"");
    }

    /**
     ESQL-Q3:------------------------------------------------------
     SELECT cust, prod, avg(x.quant), avg(y.quant)
     FROM sales
     GROUP BY cust, prod ; x, y
     SUCH THAT x.cust=cust and x.prod=prod,
               y.cust<>cust and y.prod=prod
     having avg(x.quant) > avg(y.quant)

     SQL-Q3:------------------------------------------------------
     drop view IF EXISTS V1 CASCADE;
     drop view IF EXISTS V2 CASCADE;

     create view V1 as (
     select cust, prod, avg(quant)
     from sales
     group by cust, prod
     );

     create view V2 as (
     select V1.prod, V1.cust, avg(S.quant)
     from sales S, V1
     where V1.cust <> S.cust and S.prod = V1.prod
     group by V1.prod, V1.cust
     );

     select V1.cust, V1.prod, V1.avg as avg_x, V2.avg as avg_y
     from V2, V1
     where V1.cust = V2.cust and V2.prod = V1.prod and V1.avg > V2.avg;

     RESULT:-------------------------------------------------------------------------
     cust  |  prod   |         avg_x         |         avg_y
     -------+---------+-----------------------+-----------------------
     Knuth | Bread   | 2713.5000000000000000 | 2029.3589743589743590
     Bloom | Eggs    | 2504.8181818181818182 | 2487.3421052631578947
     Bloom | Milk    | 2764.6923076923076923 | 1995.4324324324324324
     Emily | Fruits  | 2696.8461538461538462 | 2366.4400000000000000
     Sam   | Butter  | 2701.5000000000000000 | 1986.0250000000000000
     Emily | Soap    | 2411.2500000000000000 | 2199.6756756756756757
     Bloom | Butter  | 3017.0000000000000000 | 2044.4888888888888889
     Knuth | Coke    | 2726.8461538461538462 | 2456.5263157894736842
     Helen | Bread   | 2781.3333333333333333 | 2063.6904761904761905
     Helen | Yogurt  | 2655.0000000000000000 | 2395.5000000000000000
     Knuth | Cookies | 2608.1000000000000000 | 2473.3243243243243243
     Emily | Pepsi   | 3254.5714285714285714 | 2622.3829787234042553
     Emily | Cookies | 2672.8125000000000000 | 2413.8387096774193548
     Helen | Soap    | 2499.2222222222222222 | 2171.8055555555555556
     Sam   | Pepsi   | 2988.3076923076923077 | 2614.2926829268292683
     Bloom | Soap    | 2377.3333333333333333 | 2215.7435897435897436
     Emily | Milk    | 2663.3750000000000000 | 2106.3095238095238095
     Emily | Eggs    | 2756.6666666666666667 | 2405.1891891891891892
     Knuth | Eggs    | 2538.1000000000000000 | 2479.2564102564102564
     Bloom | Fruits  | 2471.2352941176470588 | 2421.0869565217391304
     Knuth | Yogurt  | 2644.6250000000000000 | 2413.2058823529411765
     Bloom | Cookies | 3069.1111111111111111 | 2367.6842105263157895
     Helen | Coke    | 2617.5333333333333333 | 2487.0555555555555556
     Emily | Butter  | 2316.8888888888888889 | 2056.4358974358974359
     Sam   | Fruits  | 3387.9000000000000000 | 2254.7547169811320755
     (25 rows)

     Helen      Coke       2617.533333      2487.055556
     Bloom      Soap       2377.333333      2215.743590
     Sam        Fruits     3387.900000      2254.754717
     Helen      Bread      2781.333333      2063.690476
     Bloom      Cookies    3069.111111      2367.684211
     Bloom      Eggs       2504.818182      2487.342105
     Bloom      Fruits     2471.235294      2421.086957
     Bloom      Milk       2764.692308      1995.432432
     Emily      Butter     2316.888889      2056.435897
     Emily      Pepsi      3254.571429      2622.382979
     Helen      Yogurt     2655.000000      2395.500000
     Knuth      Bread      2713.500000      2029.358974
     Knuth      Eggs       2538.100000      2479.256410
     Knuth      Yogurt     2644.625000      2413.205882
     Knuth      Cookies    2608.100000      2473.324324
     Emily      Soap       2411.250000      2199.675676
     Helen      Soap       2499.222222      2171.805556
     Emily      Eggs       2756.666667      2405.189189
     Bloom      Butter     3017.000000      2044.488889
     Sam        Butter     2701.500000      1986.025000
     Emily      Fruits     2696.846154      2366.440000
     Emily      Cookies    2672.812500      2413.838710
     Sam        Pepsi      2988.307692      2614.292683
     Emily      Milk       2663.375000      2106.309524
     Knuth      Coke       2726.846154      2456.526316
     */
    static void loadQ3(Query q) {
        q.tableName = "sales";

        q.gAttrs.add("cust");
        q.gAttrs.add("prod");

        q.projections.add("cust");
        q.projections.add("prod");
        q.projections.add("avg_quant_1");
        q.projections.add("avg_quant_2");

        List<String> aggFns1 = new ArrayList<>();
        List<String> aggFns2 = new ArrayList<>();
        aggFns1.add("sum_quant_1");
        aggFns1.add("cnt_quant_1");
        aggFns1.add("avg_quant_1");
        aggFns2.add("sum_quant_2");
        aggFns2.add("cnt_quant_2");
        aggFns2.add("avg_quant_2");
        q.aggFns.put(1, aggFns1);
        q.aggFns.put(2, aggFns2);

        q.suchthats.put(1, "cust_1 = cust_0 and prod_1 = prod_0");
        q.suchthats.put(2, "cust_2 <> cust_0 and prod_2 = prod_0");

        q.having = "avg_quant_1 > avg_quant_2";
    }

    /**
     ESQL-Q4:------------------------------------------------------
     SELECT prod, month, count(z.quant)
     FROM sales
     GROUP BY prod, month; x, y, z
     SUCH THAT x.prod = prod and x.month = x.month-1,
     y.prod = prod and y.month = y.month+1,
     z.prod = prod and z.month = month and z.sale > avg(x.sale) and z.sale < avg(y.sale)

     SQL-Q4:------------------------------------------------------
     drop view IF EXISTS V0 CASCADE;
     drop view IF EXISTS V1 CASCADE;
     drop view IF EXISTS V2 CASCADE;

     create view V0 as
     select prod, month
     from sales
     group by prod, month;

     create view V1 as
     select V0.prod, V0.month, avg(sales.quant) as avg
     from V0 left join sales
     on sales.prod = V0.prod and sales.month = V0.month-1
     group by V0.prod, V0.month;

     create view V2 as
     select V0.prod, V0.month, avg(sales.quant) as avg
     from V0 left join sales
     on sales.prod = V0.prod and sales.month = V0.month+1
     group by V0.prod,V0.month;

     select sales.prod, sales.month, count(sales.quant)
     from sales, V1, V2
     where sales.prod = V1.prod and sales.prod = V2.prod
     and sales.month = V1.month and sales.month = V2.month
     and sales.quant between V1.avg and V2.avg
     group by sales.prod, sales.month;

     RESULT:-------------------------------------------------------------------------
     prod   | month | count
     ---------+-------+-------
     Bread   |     8 |     3
     Butter  |     3 |     2
     Coke    |     6 |     1
     Coke    |     7 |     1
     Coke    |     9 |     1
     Cookies |     6 |     1
     Cookies |    11 |     1
     Eggs    |     4 |     2
     Eggs    |     7 |     1
     Fruits  |     4 |     5
     Fruits  |     7 |     3
     Fruits  |    10 |     1
     Milk    |     8 |     1
     Pepsi   |     9 |     2
     Soap    |    11 |     4
     Yogurt  |     5 |     1
     (16 rows)

     Fruits     7          3
     Fruits     4          5
     Cookies    11         1
     Milk       8          1
     Eggs       7          1
     Eggs       4          2
     Yogurt     5          1
     Bread      8          3
     Butter     3          2
     Pepsi      9          2
     Fruits     10         1
     Cookies    6          1
     Soap       11         4
     Coke       6          1
     Coke       7          1
     Coke       9          1
     */
    static void loadQ4(Query q) {
        q.tableName = "sales";

        q.gAttrs.add("prod");
        q.gAttrs.add("month");

        q.projections.add("prod");
        q.projections.add("month");
        q.projections.add("cnt_quant_3");

        List<String> aggFns1 = new ArrayList<>();
        List<String> aggFns2 = new ArrayList<>();
        List<String> aggFns3 = new ArrayList<>();
        aggFns1.add("sum_quant_1");
        aggFns1.add("cnt_quant_1");
        aggFns1.add("avg_quant_1");
        aggFns2.add("sum_quant_2");
        aggFns2.add("cnt_quant_2");
        aggFns2.add("avg_quant_2");
        aggFns3.add("cnt_quant_3");
        q.aggFns.put(1, aggFns1);
        q.aggFns.put(2, aggFns2);
        q.aggFns.put(3, aggFns3);

        // no space bewteen +-*/  -> eg: month_0-1 month_0+1
        q.suchthats.put(1, "prod_1 = prod_0 and month_1 = month_0-1");
        q.suchthats.put(2, "prod_2 = prod_0 and month_2 = month_0+1");
        q.suchthats.put(3, "prod_3 = prod_0 and month_3 = month_0 and quant_3 > avg_quant_1 and quant_3 < avg_quant_2");
    }

    /**
     * examples
     * @param args
     */
    public static void main(String[] args){
        Query q = new Query();
        loadPublic(q);
//        loadQ1(q);
//        loadQ2(q);
//        loadQ3(q);
        loadQ4(q);

        EMF_Gen emf = new EMF_Gen(q);
        emf.gen();
    }
}
