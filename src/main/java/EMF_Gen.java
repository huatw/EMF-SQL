import com.squareup.javapoet.*;

import javax.lang.model.element.Modifier;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EMF_Gen {
    private Query q;

    /**
     * take a Query instance
     * @param q
     */
    public EMF_Gen(Query q) {
        q.topoSort();
        if (q.topoOrder.pop() != 0) {
            System.err.println("WRONG ORDER, g0 should go first.");
        }
        this.q = q;
    }

    /**
     * generate code, based on Query instance.
     */
    void gen() {
        TypeSpec emfQuery = TypeSpec
                .classBuilder(q.className)
                .addModifiers(Modifier.PUBLIC)
                .addFields(genFields())
                .addType(genMFStructure())
                .addMethod(genMainFn())
                .addMethod(genRunFn())
                .addMethods(genCompareFns())
                .addMethods(genDbFns())
                .build();

        JavaFile javaFile = JavaFile
                .builder("", emfQuery)
                .build();

        try {
            javaFile.writeTo(System.out);
        } catch (IOException e) {
            System.err.println("BUILD failed.");
            e.printStackTrace();
        }
    }

    /**
     * generate class fields.
     * @return
     */
    private List<FieldSpec> genFields() {
        FieldSpec conn = FieldSpec.builder(Connection.class, "conn").build();
        FieldSpec ps = FieldSpec.builder(PreparedStatement.class, "ps").build();
        FieldSpec rs = FieldSpec.builder(ResultSet.class, "rs").build();
        return Arrays.asList(conn, ps, rs);
    }

    /**
     * generate run function.
     * @return
     */
    private MethodSpec genRunFn() {
        MethodSpec.Builder runFn = MethodSpec
                .methodBuilder("run")
                .returns(void.class)
                .addException(SQLException.class)
                .addStatement("$T<String, MFTable> mfTable = new $T<>()", HashMap.class, HashMap.class)
                .addStatement("rs = ps.executeQuery()")
                .beginControlFlow("while (rs.next())");

        // init mf table
        for (String gAttr : q.gAttrs) {
            runFn.addStatement("$T $L = $L($S)", q.typeMap.get(gAttr), gAttr, getFromRs(gAttr), gAttr);
        }
        runFn
                .addStatement("String key = " + String.join(" + ", q.gAttrs))
                .addStatement("MFTable row")
                .beginControlFlow("if (mfTable.containsKey(key))")
                .addStatement("row = mfTable.get(key)");

        runFn
                .nextControlFlow("else")
                .addStatement("row = new MFTable()")
                .addStatement("mfTable.put(key, row)");
        for (String gAttr : q.gAttrs) {
            runFn.addStatement("row.$L = $L", gAttr, gAttr);
        }
        runFn.endControlFlow();

        List<String> aggFns0 = q.aggFns.get(0);
        if (aggFns0 != null) {
            for (String aggFn : q.aggFns.get(0)) {
                runFn.addStatement(getAggUpdateFn(aggFn));
            }
        }

        runFn.endControlFlow();

        // loop the topo order
        for (int cur : q.topoOrder) {
            runFn
                    .addStatement("rs = ps.executeQuery()")
                    .beginControlFlow("while(rs.next())")
                    .beginControlFlow("for (MFTable row: mfTable.values())")
                    .beginControlFlow("if ($L)", transSuchthat(q.suchthats.get(cur)));

            for (String aggFn : q.aggFns.get(cur)) {
                runFn.addStatement(getAggUpdateFn(aggFn));
            }

            runFn
                    .endControlFlow()
                    .endControlFlow()
                    .endControlFlow();
        }

        // output mf table
        runFn.beginControlFlow("for (MFTable row: mfTable.values())");
        runFn.beginControlFlow("if ($L)", getProjCheckStr(q.projections));
        if (q.having != null) runFn.beginControlFlow("if ($L)", transHaving(q.having));

        for (List<String> aggFns : q.aggFns.values()) {
            aggFns.forEach(aggFn -> {
                runFn.addStatement("$T $L = row.$L", q.typeMap.get(aggFn), aggFn, aggFn);
            });
        }

        for (String proj: q.projections) {
            if (q.gAttrs.contains(proj)) {
                runFn.addStatement("System.out.printf(\"%-10s \", row.$L)", proj);
            }
            else if (q.typeMap.get(proj).equals(double.class)) {
                runFn.addStatement("System.out.printf(\"%-16f \", $L)", proj);
            }
            else {
                runFn.addStatement("System.out.printf(\"%-10s \", $L)", proj);
            }
        }
        runFn.addStatement("System.out.println()");

        if (q.having != null) runFn.endControlFlow();
        runFn.endControlFlow();
        runFn.endControlFlow();

        return runFn.build();
    }

    /**
     * generate main function.
     * @return
     */
    private MethodSpec genMainFn() {
        MethodSpec.Builder mainFn = MethodSpec
                .methodBuilder("main")
                .returns(void.class)
                .addModifiers(Modifier.PUBLIC, Modifier.STATIC)
                .addException(SQLException.class)
                .addException(ClassNotFoundException.class)
                .addParameter(String[].class, "args");

        mainFn
                .addStatement("$L q = new $L()", q.className, q.className)
                .addStatement("q.connect()")
                .addStatement("q.run()")
                .addStatement("q.close()");

        return mainFn.build();
    }

    /**
     * generate db function.
     * @return
     */
    private List<MethodSpec> genDbFns() {
        MethodSpec closeFn = MethodSpec
                .methodBuilder("close")
                .returns(void.class)
                .addException(SQLException.class)
                .addStatement("if (rs != null) rs.close()")
                .addStatement("if (ps != null) ps.close()")
                .addStatement("if (conn != null) conn.close()")
                .build();

        String selectStr = "select * from " + q.tableName;
        if (q.where != null) selectStr += " where " + q.where;

        MethodSpec connectFn = MethodSpec
                .methodBuilder("connect")
                .returns(void.class)
                .addException(SQLException.class)
                .addException(ClassNotFoundException.class)
                .addStatement("Class.forName(\"org.postgresql.Driver\")")
                .addStatement("conn = $T.getConnection($S)", DriverManager.class, q.dbName)
                .addStatement("ps = conn.prepareStatement($S)", selectStr)
                .build();

        return Arrays.asList(closeFn, connectFn);
    }

    /**
     * generate compare function.
     * @return
     */
    private List<MethodSpec> genCompareFns() {
        MethodSpec compareStrFn = MethodSpec
                .methodBuilder("compare")
                .returns(boolean.class)
                .addParameter(String.class, "str1")
                .addParameter(String.class, "str2")
                .addStatement("return str1.equals(str2)")
                .build();

        MethodSpec compareIntFn = MethodSpec
                .methodBuilder("compare")
                .returns(boolean.class)
                .addParameter(int.class, "i1")
                .addParameter(int.class, "i2")
                .addStatement("return i1 == i2")
                .build();

        MethodSpec compareDoubleFn = MethodSpec
                .methodBuilder("compare")
                .returns(boolean.class)
                .addParameter(double.class, "d1")
                .addParameter(double.class, "d2")
                .addStatement("return d1 == d2")
                .build();

        return Arrays.asList(compareStrFn, compareIntFn, compareDoubleFn);
    }

    /**
     * generate mf table structure.
     * @return
     */
    private TypeSpec genMFStructure() {
        TypeSpec.Builder mf = TypeSpec.classBuilder("MFTable");

        for (String gAttr : q.gAttrs) {
            mf.addField(FieldSpec
                    .builder(q.typeMap.get(gAttr), gAttr)
                    .build()
            );
        }

        for (List<String> aggFns : q.aggFns.values()) {
            aggFns.forEach(aggFn -> {
                FieldSpec.Builder field = FieldSpec
                        .builder(q.typeMap.get(aggFn), aggFn);

                if (aggFn.contains("min")) {
                    field.initializer("Integer.MAX_VALUE");
                }
                if (aggFn.contains("max")) {
                    field.initializer("Integer.MIN_VALUE");
                }

                mf.addField(field.build());
            });
        }

        return mf.build();
    }

    /**
     * check projection exists.
     * @param projections
     * @return
     */
    private String getProjCheckStr(List<String> projections) {
        List<String> projExists = new ArrayList<>();
        for (String proj: q.projections) {
            if (proj.startsWith("avg_")) {
                projExists.add("!compare(row." + proj + ", 0.0)");
            }
            else if (proj.startsWith("cnt_") || proj.startsWith("sum_")) {
                projExists.add("!compare(row." + proj + ", 0)");
            }
            else if (proj.startsWith("max_")) {
                projExists.add("!compare(row." + proj + ", Integer.MIN_VALUE)");
            }
            else if (proj.startsWith("min_")) {
                projExists.add("!compare(row." + proj + ", Integer.MAX_VALUE)");
            }
        }
        return String.join(" && ", projExists);
    }

    /**
     * transform having string.
     * @param having
     * @return
     */
    private String transHaving(String having) {
        String s0 = having
                .replaceAll(" +and +", " && ")
                .replaceAll(" +or +", "||");

        Pattern rRowAttr = Pattern.compile("[A-Za-z_\\d]+");
        Pattern rEq = Pattern.compile("(\\S+) *= *(\\S+)");

        String s1 = StringReplacer.replace(s0, rRowAttr, (Matcher m) -> {
            String attr = m.group();
            return "row." + attr;
        });

        String result = StringReplacer.replace(s1, rEq, (Matcher m) -> {
            String attr1 = m.group(1);
            String attr2 = m.group(2);
            return "compare(" + attr1 + ", " + attr2 + ")";
        });
        return result;
    }

    /**
     * transform suchthat string.
     * @param suchthat
     * @return
     */
    private String transSuchthat(String suchthat) {
        String s1 = suchthat
                .replaceAll(" +and +", " && ")
                .replaceAll(" +or +", "||");

        Pattern rRsAttr = Pattern.compile("([A-Za-z]+_)?([A-Za-z]+)_(\\d+)");
        Pattern rEq = Pattern.compile("(\\S+) *= *(\\S+)");
        Pattern rNeq = Pattern.compile("(\\S+) *<> *(\\S+)");

        String checkAgg = "";
        StringBuffer s2 = new StringBuffer();
        Matcher mRsAttr = rRsAttr.matcher(s1);
        while (mRsAttr.find()) {
            // avg_quant_n
            String aggFnStr = mRsAttr.group(1);
            if (aggFnStr != null && aggFnStr.contains("_")) {
                String agg = "row." + mRsAttr.group();
                switch (mRsAttr.group(1)) {
                    case "max_":
                        mRsAttr.appendReplacement(s2, agg);
                        checkAgg += "!compare(Integer.MIN_VALUE, " + agg + ") && ";
                        break;
                    case "min_":
                        mRsAttr.appendReplacement(s2, agg);
                        checkAgg += "!compare(Integer.MAX_VALUE, " + agg + ") && ";
                        break;
                    case "avg_":
                        mRsAttr.appendReplacement(s2, agg);
                        checkAgg += "!compare(0.0, " + agg + ") && ";
                        break;
                    default:
                        mRsAttr.appendReplacement(s2, agg);
                        checkAgg += "!compare(0, " + agg + ") && ";
                        break;
                }
            }
            else {
                // quant
                String attr = mRsAttr.group(2);
                // quant_0 -> row.quant
                if (mRsAttr.group(3).equals("0")) {
                    mRsAttr.appendReplacement(s2, "row." + attr);
                }
                else {
                    // quant_1 -> getInt("quant")
                    mRsAttr.appendReplacement(s2, getFromRs(attr) + "(\"" + attr + "\")");
                }
            }
        }
        mRsAttr.appendTail(s2);

        String s3 = StringReplacer.replace(s2.toString(), rEq, (Matcher m) -> {
            String attr1 = m.group(1);
            String attr2 = m.group(2);
            return "compare(" + attr1 + ", " + attr2 + ")";
        });
        String result = StringReplacer.replace(s3, rNeq, (Matcher m) -> {
            String attr1 = m.group(1);
            String attr2 = m.group(2);
            return "!compare(" + attr1 + ", " + attr2 + ")";
        });
        return checkAgg + result;
    }

    /**
     * transform aggregate update string.
     * @param aggFn
     * @return
     */
    private String getAggUpdateFn(String aggFn) {
        String[] arr = aggFn.split("_");
        if (arr[0].equals("sum")) {
            return String.format("row.%s += %s(\"%s\")", aggFn, getFromRs(aggFn), arr[1]);
        }
        if (arr[0].equals("avg")) {
            return String.format("row.%s = (double) row.sum_%s_%s / row.cnt_%s_%s", aggFn, arr[1], arr[2], arr[1], arr[2]);
        }
        if (arr[0].equals("cnt")) {
            return String.format("row.%s += 1", aggFn);
        }
        if (arr[0].equals("max")) {
            return String.format("if (row.%s < %s(\"%s\")) row.%s = %s(\"%s\")", aggFn, getFromRs(aggFn), arr[1], aggFn, getFromRs(aggFn), arr[1]);
        }
        if (arr[0].equals("min")) {
            return String.format("if (row.%s > %s(\"%s\")) row.%s = %s(\"%s\")", aggFn, getFromRs(aggFn), arr[1], aggFn, getFromRs(aggFn), arr[1]);
        }
        return "";
    }

    /**
     * get "rs.getType" by attr type.
     * @param attr
     * @return
     */
    private String getFromRs(String attr) {
        Class type = q.typeMap.get(attr);
        String rsType = "";
        if (type.equals(int.class)) rsType = "Int";
        else if (type.equals(double.class)) rsType = "Double";
        else if (type.equals(String.class)) rsType = "String";
        return "rs.get" + rsType;
    }
}

/**
 * Helper Class, Using for JS-like callback String replacing
 */
class StringReplacer {
    public static String replace(String input, Pattern regex, StringReplacerCallback callback) {
        StringBuffer resultString = new StringBuffer();
        Matcher regexMatcher = regex.matcher(input);
        while (regexMatcher.find()) {
            regexMatcher.appendReplacement(resultString, callback.replace(regexMatcher));
        }
        regexMatcher.appendTail(resultString);

        return resultString.toString();
    }
}

interface StringReplacerCallback {
    String replace(Matcher match);
}
