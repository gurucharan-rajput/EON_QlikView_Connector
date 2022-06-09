/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.QlikView;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import static jdk.nashorn.internal.objects.NativeError.printStackTrace;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author DNepak
 */
public class ExtractScriptFromLog_QlikView_v5 {

    String query = "";
    static String metadataInfo = "";

    public static void main(String[] args) {
        String path = "C:\\Users\\DNepak\\Desktop\\qlik sense\\QlikBI\\Qlik\\Qlik\\Qlik view\\Qlik View";
         //  path = "C:\\Users\\DNepak\\Desktop\\qlik sense\\New folder (2)\\New folder\\Netherlands";
        //  path="C:\\Users\\DNepak\\Desktop\\qlik sense\\demo.txt";
        //String exportPath = "C:\\Users\\DNepak\\Desktop\\qlik sense\\exported4";
        String demo = " if(root_cause_identified = '-1', 'Root cause found', 'Root cause not found') as root_cause_identified,";

        ExtractScriptFromLog_QlikView_v5 efl = new ExtractScriptFromLog_QlikView_v5();

        efl.scriptFromLOG(path);

    }

    public String scriptFromLOG(String path) {
        File filePath = new File(path);
        String exportPath = path + "//" + "Export";
        File exFile = new File(exportPath);
        Set<String> queryLists = null;

        File[] files = filePath.listFiles();
        for (File file : files) {
            System.out.println(file.getName());
            queryLists = new HashSet<>();
            String filename = file.getName();
            
            if (filename.equalsIgnoreCase("Export")) {
                continue;
            }
            ExtractScriptFromLog_QlikView_v5 esl = new ExtractScriptFromLog_QlikView_v5();
            List<String> lines = esl.extractScript(file);
            HashMap<ArrayList<String>, ArrayList<String>> queryStoreList = esl.Wrapper(lines, filename);
            ArrayList<String> queryList = null;
            ArrayList<String> storeList = null;

            for (Map.Entry<ArrayList<String>, ArrayList<String>> map : queryStoreList.entrySet()) {
                queryList = map.getKey();
                storeList = map.getValue();
            }

            int i = 1;
            HashMap<String, String> storeMap = new HashMap<>();
            int tempTableCount = 1;
            for (String query : queryList) {

                query = query.replaceFirst("\n", " ").trim();
                //  String singleLineQuery = convertQueryToSingleLine(query).trim();
                String singleLineQuery = query.trim();
                String originalLoadScript = "";
                try {
                    String sourceTableName = "";
                    String targetTableName = singleLineQuery.substring(singleLineQuery.indexOf("#*#") + 3, singleLineQuery.lastIndexOf("#*#"));
                    if (singleLineQuery.contains("#!#")) {
                        sourceTableName = singleLineQuery.substring(singleLineQuery.indexOf("#!#") + 3, singleLineQuery.lastIndexOf("#!#"));
                    }
                    if(StringUtils.containsIgnoreCase(sourceTableName,"EXCLUDE_KANAAL")){
                        continue;
                    }
                    if (StringUtils.equalsIgnoreCase(sourceTableName, "Temp")) {
                        singleLineQuery = StringUtils.replaceIgnoreCase(singleLineQuery, " temp", " Temp" + tempTableCount, StringUtils.countMatches(singleLineQuery.toLowerCase(), " temp"));
                        singleLineQuery = StringUtils.replaceIgnoreCase(singleLineQuery, "(temp)", "(Temp" + tempTableCount + ")", StringUtils.countMatches(singleLineQuery.toLowerCase(), "(temp)"));
                        singleLineQuery = StringUtils.replaceIgnoreCase(singleLineQuery, "#!#TEMP#!#", "#!#Temp" + tempTableCount + "#!#", StringUtils.countMatches(singleLineQuery.toLowerCase(), "#!#temp#!#"));
                        sourceTableName = sourceTableName + tempTableCount;
                        tempTableCount++;
                    }

                    if ((singleLineQuery.toLowerCase().startsWith("insert into ") || singleLineQuery.toLowerCase().startsWith("﻿ load ") || singleLineQuery.toLowerCase().startsWith("load")) && (singleLineQuery.toLowerCase().contains("from ") || singleLineQuery.toLowerCase().contains("from") || singleLineQuery.toLowerCase().contains(" from "))) {

                        if (StringUtils.containsIgnoreCase(sourceTableName, "QVSysConfig") || StringUtils.containsIgnoreCase(sourceTableName, "Connection") ) {
                            continue;
                        }

                        singleLineQuery = StringUtils.replaceIgnoreCase(singleLineQuery, "#*#" + targetTableName + "#*#", "");
                        singleLineQuery = StringUtils.replaceIgnoreCase(singleLineQuery, "#!#" + sourceTableName + "#!#", "");
                        originalLoadScript = singleLineQuery;
                        originalLoadScript = convertQueryToSingleLine(originalLoadScript).trim();
                        if (!singleLineQuery.toLowerCase().contains(" join")) {
                            singleLineQuery = convertQueryToSingleLine(singleLineQuery).trim();
                            singleLineQuery = singleLineQuery.toLowerCase().replaceFirst("load ", "select ");

                            //        singleLineQuery = "Insert INTO " + targetTableName.replaceAll("\\.", "_") + " " + singleLineQuery;
                            String extrimTarget = "Insert INTO " + targetTableName.replaceAll("\\.", "_") + " " + singleLineQuery;

                            String actualTableName = actualTableName(singleLineQuery);

                            extrimTarget = extrimTarget.substring(0, StringUtils.indexOfIgnoreCase(extrimTarget, " from "));
                            if (!sourceTableName.equals("")) {
                                extrimTarget = extrimTarget + " from " + sourceTableName.substring(sourceTableName.lastIndexOf(".") + 1, sourceTableName.length());
                            } else {
                                extrimTarget = extrimTarget + " from " + actualTableName;
                            }
                            if(!sourceTableName.equals("")){
                            singleLineQuery = singleLineQuery.substring(0, StringUtils.indexOfIgnoreCase(singleLineQuery, " from "));
                            singleLineQuery = extrimTarget + "\n" + "Insert INTO " + sourceTableName.replaceAll("\\.", "_") + " " + singleLineQuery + " from " + actualTableName;
                            }
                            else{
                                singleLineQuery=extrimTarget;
                            }

                            for (Map.Entry<String, String> storetTable : storeMap.entrySet()) {

                                if (StringUtils.containsIgnoreCase(actualTableName, storetTable.getKey())) {
                                    singleLineQuery = "Insert " + storetTable.getValue() + " " + singleLineQuery;
                                }
                            }
                        } else {

                            String joins[] = singleLineQuery.toLowerCase().split(" join");
                            singleLineQuery = "";
                            String columns = "";

                            for (int j = 0; j < joins.length; j++) {
                                String joinScript1 = joins[j].trim();
                                String join = "";

                                joinScript1 = joinScript1.substring(StringUtils.indexOfIgnoreCase(joinScript1.trim(), "load"));
                                if (joinScript1.contains(" ")) {
                                    join = joinScript1.substring(joinScript1.lastIndexOf(" "));
                                }

                                try {
                                    if (StringUtils.containsIgnoreCase(join, "left")) {
                                        join = "Left Join";
                                    } else if (StringUtils.containsIgnoreCase(join, "right")) {
                                        join = "Right Join";
                                    } else if (StringUtils.containsIgnoreCase(join, "Self")) {
                                        join = "Self Join";
                                    } else if (StringUtils.containsIgnoreCase(join, "Inner")) {
                                        join = "Inner Join";
                                    } else if (StringUtils.containsIgnoreCase(join, "join")) {
                                        join = "Join";
                                    } else {
                                        break;
                                    }

                                    String preJoin = joinScript1;
                                    String actualTableName = actualTableName(preJoin).trim();
                                    preJoin = preJoin.substring(0, preJoin.indexOf("from")) + " from " + actualTableName;

                                    if ((j + 1) < joins.length) {
                                        String postJoin = joins[j + 1];
                                        actualTableName = actualTableName(postJoin).trim();
                                        postJoin = postJoin.substring(0, postJoin.indexOf("from")) + " from " + actualTableName;

                                        String generateScript = joinTable(preJoin, postJoin, join);
                                        generateScript = convertQueryToSingleLine(generateScript).trim();
                                        if (columns.equals("")) {
                                            columns = generateScript.substring(StringUtils.indexOfIgnoreCase(generateScript, "load") + 4, StringUtils.indexOfIgnoreCase(generateScript, "from")).trim();
                                        } else {
                                            columns = columns + "," + generateScript.substring(StringUtils.indexOfIgnoreCase(generateScript, "load") + 4, StringUtils.indexOfIgnoreCase(generateScript, "from")).trim();
                                        }

                                        singleLineQuery = singleLineQuery + " " + "Insert INTO " + sourceTableName.replaceAll("\\.", "_") + " " + generateScript;
                                    }
                                    // singleLineQuery = "Insert INTO " + sourceTableName.replaceAll("\\.", "_") + " " + singleLineQuery;
                                } catch (Exception e) {
                                    printStackTrace(e);

                                }
                            }

                            singleLineQuery = singleLineQuery.toLowerCase().replaceAll("load ", "select ");
                            singleLineQuery = singleLineQuery.toLowerCase().replaceAll(" load", " select");
                            singleLineQuery = singleLineQuery + "\n" + "Insert INTO " + targetTableName.replaceAll("\\.", "_") + " Select " + columns + " From " + sourceTableName;
                            //queryLists.add("Insert INTO " + targetTableName.replaceAll("\\.", "_") + " Select " + columns + " From " + sourceTableName);
                        }
                        //\\\\\\\\\\
                        if (!originalLoadScript.isEmpty()) {
                            if (query.toLowerCase().contains("if(") || query.toLowerCase().contains("if (")) {
                                singleLineQuery = singleLineQuery.replaceAll("if\\(", "IFF\\(");
                                singleLineQuery = singleLineQuery.replaceAll("if \\(", "IFF\\(");
                            }
                            singleLineQuery = singleLineQuery + "##Erwin##" + originalLoadScript;
                        }
                    }
                    if (singleLineQuery.toLowerCase().startsWith("select")) {
                        singleLineQuery = convertQueryToSingleLine(singleLineQuery).trim();
                        singleLineQuery = singleLineQuery.replace("#*#" + targetTableName + "#*#", "");
                        singleLineQuery = singleLineQuery.replace("#!#" + sourceTableName + "#!#", "");
                        originalLoadScript = singleLineQuery;
                        singleLineQuery = "Insert INTO " + targetTableName.replaceAll("\\.", "_") + " " + singleLineQuery;
                        singleLineQuery = singleLineQuery + "##Erwin##" + originalLoadScript;
                    }

                } catch (Exception e) {
                    singleLineQuery = query;
                }
                queryLists.add(singleLineQuery);
            }
            if (!queryLists.isEmpty()) {
                StringBuilder querySB = new StringBuilder();
                for (String querys : queryLists) {
                    querySB.append(querys + "\n@@Erwin@@\n");
                }
//                if (querySB.toString().endsWith("\n@@Erwin@@\n")) {
//                    querySB = querySB.deleteCharAt(querySB.toString().lastIndexOf("\n@@Erwin@@\n"));
//                }

                File exportfile = new File(exportPath + "\\", filename + ".log");
                try {
                    FileUtils.writeStringToFile(exportfile, querySB.toString(), "UTF-8");
                } catch (IOException ex) {
                    Logger.getLogger(ExtractScriptFromLog_QlikView_v5.class.getName()).log(Level.SEVERE, null, ex);
                }

            }
        }
        return "Script are exported in this location:: " + exportPath;
    }

    public static String actualTableName(String singleLineQuery) {
        String actualTableName = singleLineQuery.split("from")[1];
        actualTableName = actualTableName.trim();
        actualTableName = actualTableName.substring(actualTableName.lastIndexOf("\\") + 1);
        if (actualTableName.contains("[") && actualTableName.contains("]")) {
            actualTableName = actualTableName.substring(0, actualTableName.indexOf("]") + 1);

        }
        if (actualTableName.contains(" ")) {
            actualTableName = actualTableName.substring(0, actualTableName.indexOf(" "));
        }
        if (actualTableName.contains("\n")) {
            actualTableName = actualTableName.split("\n")[0];
        }
        if (actualTableName.toLowerCase().contains("where")) {
            actualTableName = actualTableName.substring(0, actualTableName.toLowerCase().indexOf(" where"));
        }
        actualTableName = actualTableName.toLowerCase().replaceAll(".qvs", "");
        actualTableName = actualTableName.toLowerCase().replaceAll("(qvd)", "");
        actualTableName = actualTableName.toLowerCase().replaceAll(".xls", "");
        if (actualTableName.contains("\\")) {
            actualTableName = actualTableName.substring(actualTableName.lastIndexOf("\\") + 1, actualTableName.length());
            actualTableName = actualTableName.replaceFirst("[0-9].", "");
            actualTableName = actualTableName.replaceFirst(".", "");

        }
        actualTableName = actualTableName.replaceAll("\\(", "");
        actualTableName = actualTableName.replaceAll("\\)", "");
        actualTableName = actualTableName.replaceFirst("'", "");
        actualTableName = actualTableName.replace("'", "");
        actualTableName = actualTableName.replace("[", "");
        actualTableName = actualTableName.replace("]", "");
        if (actualTableName.trim().startsWith("_")) {
            actualTableName = actualTableName.substring(1);
        }

        return actualTableName.substring(actualTableName.lastIndexOf(".") + 1, actualTableName.length());
    }

    public List<String> extractScript(File filePath) {
        List<String> lines = null;
        try {

            String content = FileUtils.readFileToString(filePath, "UTF-8");
            lines = Arrays.asList(content.split("\n"));

        } catch (IOException ex) {
            Logger.getLogger(ExtractScriptFromLog_QlikView_v5.class.getName()).log(Level.SEVERE, null, ex);
        }
        return lines;
    }

    public HashMap<ArrayList<String>, ArrayList<String>> Wrapper(List<String> lines, String fileName) {
        boolean startLine = false;
        boolean endLine = false;
        boolean INLINE = false;
        boolean JOIN = false;
        ArrayList<String> queryList = new ArrayList<>();
        ArrayList<String> storeList = new ArrayList<>();
        HashMap<ArrayList<String>, ArrayList<String>> queryWithStore = new HashMap<>();
        metadataInfo = "";
        String srcTableName = "";
        String targetTableName = "";
        String storeTableName = "";
        String join = "";
        targetTableName = fileName;
        if (targetTableName.contains(" ") || targetTableName.contains(".") || targetTableName.contains("-") || targetTableName.contains("+")) {
            targetTableName = "[" + targetTableName + "]";
        }

        for (String line : lines) {
            // line = line.trim();

            line = line.replace("\r", "");
            line = removeTimestamp(line);
            line = line.replace("\t", " ");

            line = StringUtils.replaceIgnoreCase(line, "REPLACE ", "");
            line = StringUtils.replaceIgnoreCase(line, "Resident", "From ");
            line = StringUtils.replaceIgnoreCase(line, "null()", "null");
            line = StringUtils.replaceIgnoreCase(line, " DISTINCT", " ");
            line = StringUtils.replaceIgnoreCase(line, "Mapping load", "load");
            line = StringUtils.replaceIgnoreCase(line, "INNER KEEP LOAD", "load");
            //  line=line.trim();
            try {
                if (line.trim().endsWith(":")) {

                    srcTableName = line.replace(":", "").trim();
                    srcTableName = srcTableName.replaceAll("\'", "");
                    srcTableName = srcTableName.trim();
                    srcTableName = srcTableName.replaceFirst("\\s+", "");
                }

                if (StringUtils.containsIgnoreCase(line, " vSchema ")) {
                    metadataInfo = metadataInfo + "\n" + line.substring(line.indexOf("vSchema"));
                }
                if (StringUtils.containsIgnoreCase(line, " vDatabase ")) {
                    metadataInfo = metadataInfo + "\n" + line.substring(line.indexOf("vDatabase"));
                }
                if (StringUtils.containsIgnoreCase(line, " vTableName ")) {
                    metadataInfo = metadataInfo + "\n" + line.substring(line.indexOf("vTableName"));
                }
                if (StringUtils.containsIgnoreCase(line, " select ") || StringUtils.containsIgnoreCase(line, " select")) {
                    startLine = true;
                    query = "";
                    line = StringUtils.remove(line, "SQL");

                }

                if (startLine && StringUtils.containsIgnoreCase(line, " fields found: ")) {
                    endLine = true;
                }

                if (StringUtils.containsIgnoreCase(line, "load ") || StringUtils.endsWithIgnoreCase(line, " load") && !line.contains(",") || (StringUtils.containsIgnoreCase(line, " LOAD ") && !StringUtils.containsIgnoreCase(line, "lib:"))) {
                    if (startLine) {
                        query = "";
                    }
                    startLine = true;
                    if (StringUtils.containsIgnoreCase(line, " INLINE")) {
                        INLINE = true;
                        if (query.equals("")) {
                            query = line;
                        } else {
                            query = query + "\n" + line.trim();
                        }
                        query = StringUtils.replaceIgnoreCase(line, " INLINE", " From INLINE");
                        query = query.replace("[", "");
                    }
                }

                if (StringUtils.containsIgnoreCase(line, "STORE ") || StringUtils.containsIgnoreCase(line, "STORE [")) {
                    if (startLine) {
                        query = "";
                    }
                    storeTableName = line.substring(StringUtils.indexOfIgnoreCase(line, "store") + 5, StringUtils.indexOfIgnoreCase(line, "into")).trim();
                    storeList.add(storeTableName);
                    //   queryList.add(line);

                }

                if ((StringUtils.containsIgnoreCase(line, "LEFT JOIN") || StringUtils.containsIgnoreCase(line, "INNER JOIN")) && !StringUtils.containsIgnoreCase(query, "select")) {
                    JOIN = true;
                    join = line;
                }

                if (startLine && !endLine) {
                    line = line.replace(".qvd", "");
                    line = line.replace(".csv", "");
                    line = line.replace(".xlsx", "");

                    if (INLINE) {

                        if (line.contains("]")) {
                            INLINE = false;
                        }

                    } else {
                        if (query.equals("")) {
                            query = line.trim();
                        } else {
                            query = query + "\n" + line.trim();
                        }

                    }

                }

                if (startLine && endLine) {

                    if (StringUtils.containsIgnoreCase(line, "fields found:")) {
                        line = line.substring(StringUtils.indexOfIgnoreCase(line, "fields found:"), StringUtils.lastIndexOfIgnoreCase(line, ","));
                        line = StringUtils.removeIgnoreCase(line, "fields found:");
                        String columns[] = line.split(",");
                        line = "";
                        for (String column : columns) {
                            column = column.trim();

                            if (column.contains(" ") || column.equalsIgnoreCase("order") || column.equalsIgnoreCase("begin") || column.equalsIgnoreCase("end") || column.equalsIgnoreCase("group")) {
                                if (line.isEmpty()) {
                                    line = "[" + column + "]";
                                } else {
                                    line = line + ",\n" + "[" + column + "]";
                                }

                            } else {
                                if (line.isEmpty()) {
                                    line = column;
                                } else {
                                    line = line = line + ",\n" + column;
                                }

                            }
                        }
                        query = query.replaceAll("( )+", " ");
                        query = query.trim();
                        query = StringUtils.replaceIgnoreCase(query, "Load *", "Load " + line);
                        query = StringUtils.replaceIgnoreCase(query, "LOAD\n*", "Load " + line);
                        query = StringUtils.replaceIgnoreCase(query, "SELECT *", "Select " + line);
                        query = StringUtils.replaceIgnoreCase(query, " SELECT  *", "Select " + line);
                        query = StringUtils.replaceIgnoreCase(query, "SELECT\n *", "Select " + line);
                        query = StringUtils.replaceIgnoreCase(query, srcTableName.concat(".*"), line);

                        query.trim();
                        line = "";
                    }
                    if (query.equals("")) {
                        query = line;
                    } else {
                        query = query + "\n" + line.trim();
                    }

                    if (StringUtils.containsIgnoreCase(query, "load *") || StringUtils.containsIgnoreCase(query, "select *")) {
                        continue;
                    }
                    query = removeUnwantedText(query);
                    if (query != "") {
                        if (StringUtils.containsIgnoreCase(query, "From")) {
                            // String originalTableName = query.toLowerCase().split("from")[0];
                            if (srcTableName.contains(" ") && !srcTableName.equals("") && !srcTableName.contains("[")) {
                                srcTableName = "[" + srcTableName + "]";
                            }
                            if (StringUtils.containsIgnoreCase(query, "Load") && !StringUtils.containsIgnoreCase(query, " inline") ) {
                                if (srcTableName.equals("")) {
                                    query = "";
                                    continue;
                                }
                                // query = originalTableName.trim() + " from " + srcTableName;
                                query = query + "\n" + "#!#" + srcTableName + "#!#";
                            }
                            if(StringUtils.containsIgnoreCase(query, " inline")){
                                query=StringUtils.replaceIgnoreCase(query, " INLINE", " "+srcTableName);
                            }
                            query = query + "\n" + " #*#" + targetTableName + "#*#";

                            queryList.add(query);

                            if (JOIN) {
                                String postJOIN = queryList.get(queryList.size() - 1);
                                queryList.remove(queryList.size() - 1);
                                String preJOIN = queryList.get(queryList.size() - 1);
                                preJOIN = preJOIN.replace(preJOIN.substring(preJOIN.indexOf("#*#"), preJOIN.lastIndexOf("#*#") + 3), "");
                                if(preJOIN.contains("#!#")){
                                preJOIN = preJOIN.replace(preJOIN.substring(preJOIN.indexOf("#!#"), preJOIN.lastIndexOf("#!#") + 3), "");
                                }
                                queryList.remove(queryList.size() - 1);
                                queryList.add(preJOIN + "\n" + join + "\n" + postJOIN);
                                JOIN = false;
                            }
                        }
                    }
                    query = "";
                    startLine = false;
                    endLine = false;
                    INLINE = false;
                }
            } catch (Exception e) {
                    System.out.println("Error in::"+query);
                printStackTrace(e);

            }
        }
        queryWithStore.put(queryList, storeList);
        return queryWithStore;
    }

    public String joinTable(String preJoin, String postJoin, String join) {
        String preColumns[] = null;
        String postColumns[] = null;

        String preTable = preJoin.toLowerCase().split("from")[1].trim();
        String postTable = postJoin.toLowerCase().split("from")[1];
        postTable = actualTableName(postJoin);

        String preCol = preJoin.substring(StringUtils.indexOfIgnoreCase(preJoin, "load") + 4, StringUtils.indexOfIgnoreCase(preJoin, "from"));
        String postCol = postJoin.substring(StringUtils.indexOfIgnoreCase(postJoin, "load") + 4, StringUtils.indexOfIgnoreCase(postJoin, "from"));
        String finalPreCol = "";
        String finalPostCol = "";
        preColumns = preCol.trim().split("\n");
        boolean column = false;
        boolean string = false;
        boolean startColumn = true;
        boolean number = false;
        for (String col : preColumns) {
            if (col.contains("as ")) {
                if (StringUtils.isNumeric(col.substring(0, StringUtils.indexOfIgnoreCase(col, "as ")).trim())) {
                    number = true;
                }
            }
            if (col.trim().equals("")) {
                continue;
            }
            if (col.trim().startsWith("'")) {
                string = true;
            }

            if (col.contains("(") || col.contains(")")) {
                if (!col.contains("as ") && startColumn) {
                    column = false;
                    startColumn = false;

                } else if (col.contains("as ")) {
                    column = true;
                    startColumn = true;
                    if (StringUtils.countMatches(col, "(") == StringUtils.countMatches(col, ")") && !col.trim().startsWith("(")) {
                        column = false;
                    }
                }
            }

            if (string || column || number) {
                if (finalPreCol.equals("")) {
                    finalPreCol = col.trim();
                } else {
                    finalPreCol = finalPreCol + col.trim();
                }
                string = false;
                column = false;
                number = false;

            } else {
                if (finalPreCol.equals("")) {
                    finalPreCol = preTable + "." + col.trim();
                } else {
                    finalPreCol = finalPreCol + preTable + "." + col.trim();
                }
            }

        }
        postColumns = postCol.trim().split("\n");
        column = false;
        startColumn = true;
        for (String col : postColumns) {
            if (col.contains("as ")) {
                if (StringUtils.isNumeric(col.substring(0, StringUtils.indexOfIgnoreCase(col, "as ")).trim())) {
                    number = true;
                }
            }
            if (col.trim().equals("")) {
                continue;
            }
            if (col.trim().startsWith("'")) {
                string = true;
            }

            if (col.contains("(") || col.contains(")")) {
                if (!col.contains("as ") && startColumn) {
                    column = false;
                    startColumn = false;

                } else if (col.contains("as ")) {
                    column = true;
                    startColumn = true;
                    if (StringUtils.countMatches(col, "(") == StringUtils.countMatches(col, ")") && !col.trim().startsWith("(")) {
                        column = false;
                    }
                } else {
                    column = true;
                }
            }

            if (string || column || number) {
                if (finalPostCol.equals("")) {
                    finalPostCol = col.trim();
                } else {
                    finalPostCol = finalPostCol + col.trim();
                }

            } else {
                if (finalPostCol.equals("")) {
                    finalPostCol = postTable + "." + col.trim();
                } else {
                    finalPostCol = finalPostCol + postTable + "." + col.trim();
                }
            }
        }
        finalPreCol = finalPreCol + "," + finalPostCol;
        preJoin = preJoin.replace(preCol, " " + finalPreCol + " ");
        return preJoin + " " + join + " " + postTable + " on " + preTable + "=" + postTable;

    }

    public String findStoreTable(String content) {
        String storeTableName = content.substring(StringUtils.indexOfIgnoreCase(content, "store"), StringUtils.indexOfIgnoreCase(content, "into"));
        return null;

    }

    public String removeUnwantedText(String query) {
        boolean startLine = false;
        boolean endLine = false;
        boolean INLINE = false;
        boolean whereKeyword = false;
        String[] lines = query.split("\n");
        query = "";
        String ifConditionStmt = "";
        String endConditionStmt = "";
        boolean ifCondition = false;
        for (String line : lines) {
            line = line.replace("%", "");
            line = line.trim();
            if (StringUtils.endsWithIgnoreCase(line, "load") || StringUtils.containsIgnoreCase(line, "LOAD ")) {
                startLine = true;
                ifCondition = false;
                if (StringUtils.containsIgnoreCase(line, " INLINE [")) {
                    INLINE = true;
                }
                if (StringUtils.containsIgnoreCase(line, "Concatenate LOAD")) {
                    line = StringUtils.replaceIgnoreCase(line, "Concatenate LOAD", "LOAD");
                }
            }
            if (StringUtils.containsIgnoreCase(line, "select ") || StringUtils.containsIgnoreCase(line, "select")) {
                startLine = true;
                ifCondition = false;
            }
            if (startLine && (StringUtils.containsIgnoreCase(line, "\\\\") || StringUtils.containsIgnoreCase(line, "[lib:") || StringUtils.containsIgnoreCase(line, " lib:"))) {
                endLine = true;
                ifCondition = false;
            }
            if (startLine && !endLine) {
                if (line.startsWith("'") && StringUtils.containsIgnoreCase(line, " as ")) {
                    String column = line.split("'")[1];
                    if (!column.contains(" ")) {
                        line = line.replaceAll("'", "");
                    }
                }
                if (line.startsWith("(") && StringUtils.containsIgnoreCase(line, " as ")) {
                    String column = StringUtils.substringBetween(line, "(", ")");
                    if (!column.contains(" ")) {
                        line = line.replaceAll("\\(", "");
                        line = line.replaceAll("\\)", "");
                    }
                }
                if (INLINE) {
                    if (line.contains("]")) {
                        endLine = true;
                    } else {
                        query = query + "\n" + line;
                    }
                } else {
                    query = query + "\n" + line;
                }

            }
            if (startLine && endLine) {
                try {
                    line = line.replace(line.substring(StringUtils.indexOfIgnoreCase(line, "lib:"), StringUtils.lastIndexOf(line, "\\") + 1), "");
                } catch (Exception e) {
                }
                try {
                    line = line.replace(line.substring(StringUtils.indexOfIgnoreCase(line, "lib:"), StringUtils.lastIndexOf(line, "/") + 1), "");
                } catch (Exception e) {
                }

                line = StringUtils.removeIgnoreCase(line, "(txt, codepage is , embedded labels, delimiter is '|', msq)");

                query = query + "\n" + line;
                startLine = false;
                endLine = false;
            }
        }
        return query.replace("(qvd)", "");
    }

    public ArrayList<String> metadataInfo() {
        return null;

    }

    public String removeTimestamp(String content) {
        //To remove  TimeStamp
        //  content = content.replaceAll("[0-9-]{5}[0-9-]{3}[0-9 ]{3}[0-9:]{3}[0-9 ]{3}[0-9 ]{3}[0-9 ]{5}", "");
        content = content.replaceAll("[0-9-]{5}[0-9-]{3}[0-9 ]{3}[0-9:]{3}[0-9:]{3}[0-9 ]{3}", "");
        content = content.replaceAll("[0-9T]{9}[0-9.]{7}[0-9+]{4}[0-9 ]{5}", "");
        content = content.replaceFirst("[0-9]{4} ", " ");
        // content = content.replaceFirst("( )+", "");
        return content;

    }

    public String convertQueryToSingleLine(String query) {
        String singleLineQuery = "";
        String queryLines[] = query.split("\n");
        for (String queryline : queryLines) {
            singleLineQuery = singleLineQuery + " " + queryline;
//            if(!queryline.isEmpty()){
//            singleLineQuery = singleLineQuery + " " + queryline;
//            }
        }
        singleLineQuery = singleLineQuery.replaceAll("\\s+", " ");
        return singleLineQuery.trim();

    }

}
