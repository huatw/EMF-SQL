package generated;

import java.lang.ClassNotFoundException;
import java.lang.String;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

public class Q1 {
    Connection conn;

    PreparedStatement ps;

    ResultSet rs;

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        Q1 q = new Q1();
        q.connect();
        q.run();
        q.close();
    }

    void run() throws SQLException {
        HashMap<String, MFTable> mfTable = new HashMap<>();
        rs = ps.executeQuery();
        while (rs.next()) {
            String cust = rs.getString("cust");
            int month = rs.getInt("month");
            String key = cust + month;
            MFTable row;
            if (mfTable.containsKey(key)) {
                row = mfTable.get(key);
            } else {
                row = new MFTable();
                mfTable.put(key, row);
                row.cust = cust;
                row.month = month;
            }
            row.sum_quant_0 += rs.getInt("quant");
            row.cnt_quant_0 += 1;
            row.avg_quant_0 = (double) row.sum_quant_0 / row.cnt_quant_0;
            if (row.max_quant_0 < rs.getInt("quant")) row.max_quant_0 = rs.getInt("quant");
        }
        rs = ps.executeQuery();
        while(rs.next()) {
            for (MFTable row: mfTable.values()) {
                if (compare(rs.getString("cust"), row.cust) && rs.getInt("month") < row.month) {
                    row.sum_quant_1 += rs.getInt("quant");
                    row.cnt_quant_1 += 1;
                    row.avg_quant_1 = (double) row.sum_quant_1 / row.cnt_quant_1;
                    if (row.max_quant_1 < rs.getInt("quant")) row.max_quant_1 = rs.getInt("quant");
                }
            }
        }
        rs = ps.executeQuery();
        while(rs.next()) {
            for (MFTable row: mfTable.values()) {
                if (compare(rs.getString("cust"), row.cust) && rs.getInt("month") > row.month) {
                    row.sum_quant_2 += rs.getInt("quant");
                    row.cnt_quant_2 += 1;
                    row.avg_quant_2 = (double) row.sum_quant_2 / row.cnt_quant_2;
                    if (row.max_quant_2 < rs.getInt("quant")) row.max_quant_2 = rs.getInt("quant");
                }
            }
        }
        for (MFTable row: mfTable.values()) {
            if (!compare(row.avg_quant_1, 0.0) && !compare(row.avg_quant_0, 0.0) && !compare(row.avg_quant_2, 0.0)) {
                if (row.avg_quant_1 < row.avg_quant_2) {
                    System.out.printf("%-10s ", row.cust);
                    System.out.printf("%-10s ", row.month);
                    System.out.printf("%-16f ", row.avg_quant_1);
                    System.out.printf("%-16f ", row.avg_quant_0);
                    System.out.printf("%-16f ", row.avg_quant_2);
                    System.out.println();
                }
            }
        }
    }

    boolean compare(String str1, String str2) {
        return str1.equals(str2);
    }

    boolean compare(int i1, int i2) {
        return i1 == i2;
    }

    boolean compare(double d1, double d2) {
        return d1 == d2;
    }

    void close() throws SQLException {
        if (rs != null) rs.close();
        if (ps != null) ps.close();
        if (conn != null) conn.close();
    }

    void connect() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/emfquery");
        ps = conn.prepareStatement("select * from sales where year=1990");
    }

    class MFTable {
        String cust;

        int month;

        int sum_quant_0;

        int cnt_quant_0;

        double avg_quant_0;

        int max_quant_0 = Integer.MIN_VALUE;

        int sum_quant_1;

        int cnt_quant_1;

        double avg_quant_1;

        int max_quant_1 = Integer.MIN_VALUE;

        int sum_quant_2;

        int cnt_quant_2;

        double avg_quant_2;

        int max_quant_2 = Integer.MIN_VALUE;
    }
}
