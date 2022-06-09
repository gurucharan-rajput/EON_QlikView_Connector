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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author DNepak
 */
public class ExtractScriptFromLog_QlikView {

    String query = "";
    static String metadataInfo = "";

    public static void main(String[] args) {
        String path = "C:\\Users\\DNepak\\Desktop\\qlik sense\\QlikBI\\Qlik\\Qlik\\Qlik view\\Qlik View";
        //     path = "C:\\Users\\DNepak\\Desktop\\qlik sense\\New folder (2)\\New folder\\Netherlands";
        //  path="C:\\Users\\DNepak\\Desktop\\qlik sense\\demo.txt";
        //String exportPath = "C:\\Users\\DNepak\\Desktop\\qlik sense\\exported4";
        String demo = " if(root_cause_identified = '-1', 'Root cause found', 'Root cause not found') as root_cause_identified,";

        ExtractScriptFromLog_QlikView efl = new ExtractScriptFromLog_QlikView();

        efl.scriptFromLOG(path);

    }

    public String scriptFromLOG(String path) {
        File filePath = new File(path);
        String exportPath = path + "//" + "Export";
        File exFile = new File(exportPath);
        Set<String> queryLists = null;

        File[] files = filePath.listFiles();
        for (File file : files) {
            queryLists = new HashSet<>();
            String filename = file.getName();
            if (filename.equalsIgnoreCase("Export")) {
                continue;
            }
            filename = filename.substring(0, filename.lastIndexOf("."));
            ExtractScriptFromLog_QlikView esl = new ExtractScriptFromLog_QlikView();
            List<String> lines = esl.extractScript(file);
            HashMap<ArrayList<String>, ArrayList<String>> queryStoreList = esl.Wrapper(lines, filename);
            ArrayList<String> queryList = null;
            ArrayList<String> storeList = null;

            for (Map.Entry<ArrayList<String>, ArrayList<String>> map : queryStoreList.entrySet()) {
                queryList = map.getKey();
                storeList = map.getValue();
            }

            int i = 1;
            //    File exportfile = new File(exportPath + "\\" + filename, filename + "_" + "Metadata" + ".log");
            //    File exportfile = new File(exportPath + "\\", filename + "_" + "Metadata" + ".log");
            HashMap<String, String> storeMap = new HashMap<>();

            for (String query : queryList) {

                query = query.replaceFirst("\n", " ").trim();
                String singleLineQuery = convertQueryToSingleLine(query).trim();
                String originalLoadScript = "";
                try {

                    if ((singleLineQuery.toLowerCase().startsWith("insert into ") || singleLineQuery.toLowerCase().startsWith("﻿ load ") || singleLineQuery.toLowerCase().startsWith("load") && singleLineQuery.toLowerCase().contains(" from "))) {
                        String sourceTableName = "";
                        String targetTableName = singleLineQuery.substring(singleLineQuery.indexOf("#*#") + 3, singleLineQuery.lastIndexOf("#*#"));
                        if (singleLineQuery.contains("#!#")) {
                            sourceTableName = singleLineQuery.substring(singleLineQuery.indexOf("#!#") + 3, singleLineQuery.lastIndexOf("#!#"));
                        }

//                        if (!storeList.contains(sourceTableName)) {
//                            continue;
//                        }
                        if (StringUtils.containsIgnoreCase(singleLineQuery, "QVSysConfig")) {
                            continue;
                        }
                        singleLineQuery = singleLineQuery.replace("#*#" + targetTableName + "#*#", "");
                        singleLineQuery = singleLineQuery.replace("#!#" + sourceTableName + "#!#", "");
                        originalLoadScript = singleLineQuery;
                        if (!singleLineQuery.toLowerCase().contains(" join")) {
                            if (!StringUtils.containsIgnoreCase(singleLineQuery, " INLINE") && !storeList.contains(sourceTableName)) {
                                continue;
                            }

                            singleLineQuery = "Insert INTO " + targetTableName.replaceAll("\\.", "_") + " " + singleLineQuery;
                            singleLineQuery = singleLineQuery.toLowerCase().replaceFirst("load ", "select ");
                            String actualTableName = actualTableName(singleLineQuery);

                            singleLineQuery = singleLineQuery.substring(0, singleLineQuery.indexOf(" from "));
                            if (!sourceTableName.equals("")) {
                                singleLineQuery = singleLineQuery + " from " + sourceTableName.substring(sourceTableName.lastIndexOf(".") + 1, sourceTableName.length());
                            } else {
                                singleLineQuery = singleLineQuery + " from " + actualTableName;
                            }

                            for (Map.Entry<String, String> storetTable : storeMap.entrySet()) {

                                if (StringUtils.containsIgnoreCase(actualTableName, storetTable.getKey())) {
                                    singleLineQuery = "Insert " + storetTable.getValue() + " " + singleLineQuery;
                                }
                            }
                        } else  {
                            
                            String join = "";
                            if (StringUtils.containsIgnoreCase(singleLineQuery, "left join")) {
                                join = "Left Join";
                            } else if (StringUtils.containsIgnoreCase(singleLineQuery, "right join")) {
                                join = "Right Join";
                            } else if (StringUtils.containsIgnoreCase(singleLineQuery, "Self join")) {
                                join = "Self Join";
                            } else {
                                join = "Join";
                            }
                            String preJoin = singleLineQuery.toLowerCase().split(join.toLowerCase())[0];
                            String actualTableName = actualTableName(preJoin);
                            preJoin = preJoin.substring(0, preJoin.indexOf(" from ")) + " from " + actualTableName;

                            String postJoin = singleLineQuery.toLowerCase().split(join.toLowerCase())[1];
                            actualTableName = actualTableName(postJoin);
                            postJoin = postJoin.substring(0, postJoin.indexOf(" from ")) + " from " + actualTableName;


                            originalLoadScript = singleLineQuery;
                            singleLineQuery = joinTable(preJoin, postJoin, join);
                            singleLineQuery = "Insert INTO " + sourceTableName.replaceAll("\\.", "_") + " " + singleLineQuery;
                            String columns = singleLineQuery.substring(StringUtils.indexOfIgnoreCase(singleLineQuery, "load") + 4, StringUtils.indexOfIgnoreCase(singleLineQuery, "from")).trim();
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
                            String targetTableName = singleLineQuery.substring(singleLineQuery.indexOf("#*#") + 3, singleLineQuery.lastIndexOf("#*#"));
                            singleLineQuery = singleLineQuery.replace("#*#" + targetTableName + "#*#", "");
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
                    Logger.getLogger(ExtractScriptFromLog_QlikView.class.getName()).log(Level.SEVERE, null, ex);
                }

            }
        }
        return "Script are exported in this location:: " + exportPath;
    }

    public static String actualTableName(String singleLineQuery) {
        //  String actualTableName = singleLineQuery.substring(singleLineQuery.toLowerCase().indexOf(" from ") + 5);
        String actualTableName = singleLineQuery.substring(StringUtils.indexOfIgnoreCase(singleLineQuery, " from ") + 5);
        actualTableName = actualTableName.trim();
        actualTableName = actualTableName.substring(actualTableName.lastIndexOf("\\") + 1);
        if (actualTableName.contains("[") && actualTableName.contains("]")) {
            actualTableName = actualTableName.substring(0, actualTableName.indexOf("]") + 1);

        } else if (actualTableName.contains(" ")) {
            actualTableName = actualTableName.substring(0, actualTableName.indexOf(" "));
        }
        if (actualTableName.toLowerCase().contains("where")) {
            actualTableName = actualTableName.substring(0, actualTableName.toLowerCase().indexOf(" where"));
        }
        actualTableName = actualTableName.toLowerCase().replaceAll(".qvs", "");
        actualTableName = actualTableName.toLowerCase().replaceAll("(qvd)", "");
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
//                        if (actualTableName.startsWith("\\\\")) {
//                            actualTableName = "[" + actualTableName + "]";
//                        }
        if (actualTableName.trim().startsWith("_")) {
            actualTableName = actualTableName.substring(1);
        }

        return actualTableName.substring(actualTableName.lastIndexOf(".") + 1, actualTableName.length());
    }

    public List<String> extractScript(File filePath) {
        List<String> lines = null;
        try {

            String content = FileUtils.readFileToString(filePath, "UTF-8");
            content = removeTimestamp(content);
            //   String metaData=content.substring(content.indexOf("vDatabase"),content.indexOf("\n"));

            lines = Arrays.asList(content.split("\n"));

        } catch (IOException ex) {
            Logger.getLogger(ExtractScriptFromLog_QlikView.class.getName()).log(Level.SEVERE, null, ex);
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
            //  line = removeTimestamp(line);
            line = line.replace("\t", " ");

            line = StringUtils.replaceIgnoreCase(line, "REPLACE ", "");
            line = StringUtils.replaceIgnoreCase(line, "Resident", "From ");
            line = StringUtils.replaceIgnoreCase(line, "null()", "null");
            line = StringUtils.replaceIgnoreCase(line, " DISTINCT", " ");
            line = StringUtils.replaceIgnoreCase(line, "Mapping load", "load");
            //  line=line.trim();

            if (line.trim().endsWith(":")) {

                srcTableName = line.replace(":", "").trim();
                srcTableName = srcTableName.replaceAll("\'", "");
                if (srcTableName.contains("[")) {
                    srcTableName = "";
                }

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

            if (StringUtils.containsIgnoreCase(line, "LEFT JOIN") || StringUtils.containsIgnoreCase(line, "INNER JOIN")) {
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
                        if (srcTableName.contains(" ") && !srcTableName.equals("")) {
                            srcTableName = "[" + srcTableName + "]";
                        }
                        if (StringUtils.containsIgnoreCase(query, "Load")) {
                            if (srcTableName.equals("")) {
                                query = "";
                                continue;
                            }
                            // query = originalTableName.trim() + " from " + srcTableName;
                            query = query + "\n" + "#!#" + srcTableName + "#!#";
                        }
                        query = query + "\n" + " #*#" + targetTableName + "#*#";

                        queryList.add(query);

                        if (JOIN) {
                            String postJOIN = queryList.get(queryList.size() - 1);
                            queryList.remove(queryList.size() - 1);
                            String preJOIN = queryList.get(queryList.size() - 1);
                            preJOIN = preJOIN.replace(preJOIN.substring(preJOIN.indexOf("#*#"), preJOIN.lastIndexOf("#*#") + 3), "");
                            preJOIN = preJOIN.replace(preJOIN.substring(preJOIN.indexOf("#!#"), preJOIN.lastIndexOf("#!#") + 3), "");
                            queryList.remove(queryList.size() - 1);
//                            query=joinTable(preJOIN, postJOIN, join);
//                            query=query + "\n" + "#!#" + srcTableName + "#!#";
//                            query=query + "\n" + " #*#" + targetTableName + "#*#";
                            queryList.add(preJOIN + "\n" + join + "\n" + postJOIN);
                            JOIN=false;
                        }
                    }
                }
                query = "";
                startLine = false;
                endLine = false;
                INLINE = false;
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
        //postTable = postTable.replace(postTable.substring(postTable.indexOf("#*#"), postTable.lastIndexOf("#*#") + 3), "");
        //postTable = postTable.replace(postTable.substring(postTable.indexOf("#!#"), postTable.lastIndexOf("#!#") + 3), "").trim();
        String preCol = preJoin.substring(StringUtils.indexOfIgnoreCase(preJoin, "load") + 4, StringUtils.indexOfIgnoreCase(preJoin, "from"));
        String postCol = postJoin.substring(StringUtils.indexOfIgnoreCase(postJoin, "load") + 4, StringUtils.indexOfIgnoreCase(postJoin, "from"));
        String finalPreCol = "";
        String finalPostCol = "";
        preColumns = preCol.split(",");
        for (String col : preColumns) {
            if (finalPreCol.equals("")) {
                finalPreCol = preTable + "." + col.trim();
            } else {
                finalPreCol = finalPreCol + "," + preTable + "." + col.trim();
            }
        }
        postColumns = postCol.split(",");
        for (String col : postColumns) {
            if (finalPostCol.equals("")) {
                finalPostCol = postTable + "." + col.trim();
            } else {
                finalPostCol = finalPostCol + "," + postTable + "." + col.trim();
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
            if (line.contains("%")) {
                System.out.println("com.erwin.extractingscript_v2.ExtractScriptFromLog_QlikView_v3.removeUnwantedText()");
            }

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
        content = content.replaceAll("[0-9]{4} ", " ");
        content = content.replaceFirst("( )+", "");
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
