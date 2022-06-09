package com.erwin.cfx.QlikView.connectors.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class ParsingEGT {

    public static boolean flag = false;
    public Map<String, List<String>> hm = new HashMap();

    public static void main(String[] args) throws IOException {
        String tempColumn = "";
        Map<String, List<String>> hm = new HashMap();
        List<String> functions = new ArrayList();
        List<String> columnName = new ArrayList();

        List<String> lines = FileUtils.readLines(new File("C:\\\\Users\\\\MKurugod\\\\Downloads\\\\a2.txt"), "UTF-8");
        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains("<MyFun> ::=")) {
                String functionBuild = "";

                functionBuild = getFunction(lines, i);
                if (functionBuild != null && functionBuild != "") {
                    functions.add(functionBuild);
                }
                for (int j = i + 1; j < lines.size(); j++) {
                    if (lines.get(j).contains("<Statement> ::= '[' <Statements> ']'")) {
                        j = getSquareBraceData(lines, j, columnName);
                    } else if (lines.get(j).contains("<Expression> ::= Identifier")) {
                        getStringLiteral(lines, j, columnName);
                    } else if (lines.get(j).contains("<Expression> ::= StringLiteral")) {
                        getStringLiteral(lines, j, columnName);
                    }
                    if (lines.get(j).contains(")")) {
                        i = j + 1;
                        flag = true;
                        break;
                    }
                }
            }

            if (lines.get(i).contains("'[' <Statements> '_' <Statements> ']'")) {
                i = getSquareBraceData(lines, i, columnName);
            } else if (lines.get(i).contains("<Statement> ::= '[' <Statements> ']'")) {
                i = getSquareBraceData(lines, i, columnName);
            } else if (lines.get(i).contains("<Expression> ::= Identifier")) {
                getStringLiteral(lines, i, columnName);
            } else if (lines.get(i).contains("<Expression> ::= StringLiteral")) {
                getStringLiteral(lines, i, columnName);
            }

        }
        String tableName = "";
        for (int i = 0; i < columnName.size(); i++) {
            if (columnName.get(i).equalsIgnoreCase("as")) {
                columnName.remove(i);
                columnName.remove(i);
            } else if (columnName.get(i).equalsIgnoreCase("from")) {
                columnName.remove(i);
                tableName = columnName.get(i);
                columnName.remove(i);
            }
        }
        hm.put(tableName, columnName);
        hm.put("functions", functions);
        System.out.println(hm);

    }

    public Map<String, List<String>> getTableInfo(String qlikTree) {
        Map<String, List<String>> hm = new HashMap();
        List<String> functions = new ArrayList();
        List<String> columnName = new ArrayList();

        String tempColumn = "";
        String[] line = qlikTree.split("\n");
        List<String> lines = new ArrayList<>();
        for (String con : line) {
            lines.add(con);
        }

        for (int i = 0; i < lines.size(); i++) {
            if (lines.get(i).contains("<MyFun> ::=")) {
                String functionBuild = "";

                functionBuild = getFunction(lines, i);
                if (functionBuild != null && functionBuild != "") {
                    functions.add(functionBuild);
                }
                for (int j = i + 1; j < lines.size(); j++) {
                    if (lines.get(j).contains("<Statement> ::= '[' <Statements> ']'")) {
                        j = getSquareBraceData(lines, j, columnName);
                    } else if (lines.get(j).contains("<Expression> ::= Identifier")) {
                        getStringLiteral(lines, j, columnName);
                    } else if (lines.get(j).contains("<Expression> ::= StringLiteral")) {
                        getStringLiteral(lines, j, columnName);
                    }
                    if (lines.get(j).contains(")")) {
                        i = j + 1;
                        flag = true;
                        break;
                    }
                }
            }

            if (lines.get(i).contains("'[' <Statements> '_' <Statements> ']'")) {
                i = getSquareBraceData(lines, i, columnName);
            } else if (lines.get(i).contains("<Statement> ::= '[' <Statements> ']'")) {
                i = getSquareBraceData(lines, i, columnName);
            } else if (lines.get(i).contains("<Expression> ::= Identifier")) {
                getStringLiteral(lines, i, columnName);
            } else if (lines.get(i).contains("<Expression> ::= StringLiteral")) {
                getStringLiteral(lines, i, columnName);
            }

        }
        String tableName = "";
        for (int i = 0; i < columnName.size(); i++) {

            if (columnName.get(i).equalsIgnoreCase("as")) {
                columnName.remove(i);
                columnName.remove(i);
            } else if (columnName.get(i).equalsIgnoreCase("from")) {
                columnName.remove(i);
                tableName = columnName.get(i);
                columnName.remove(i);
            }
        }

        hm.put(tableName, columnName);
        hm.put("functions", functions);
        System.out.println(hm);
        return hm;
    }

    public static int getSquareBraceData(List<String> lines, int i, List<String> columnName) {
        String tempCol = "";
        for (int j = i + 1; j < lines.size(); j++) {
            if (lines.get(j).contains("<Expression> ::= Identifier")) {
                if (lines.get(j + 2).contains("_")) {
                    tempCol = tempCol + lines.get(j + 1).replace("|", "").replace("+-", "").trim()
                            + lines.get(j + 2).replace("|", "").replace("+-", "").trim();
                } else {
                    if (lines.get(j + 2).contains("<Expression> ::= Identifier")) {
                        tempCol = tempCol + " " + lines.get(j + 1).replace("|", "").replace("+-", "").trim() + " ";
                    } else {
                        tempCol = tempCol + lines.get(j + 1).replace("|", "").replace("+-", "").trim();
                    }

                }
            } else if (lines.get(j).contains("]")) {
                i = j + 1;
                columnName.add(tempCol);
                flag = false;
                break;
            }
        }
        return i;
    }// method end

    public static void getStringLiteral(List<String> lines, int i, List<String> columnName) {
        columnName.add(lines.get(i + 1).replace("|", "").replace("+-", "").trim());
    }

    public static String getFunction(List<String> lines, int i) {
        String functionBuild = "";
        for (int j = i + 1; j < lines.size(); j++) {
            if (lines.get(j).contains("<Statements> '&' <Statements>")) {
                continue;
            } else if (lines.get(j).contains("'[' <Statements> ']'")) {
                continue;
            } else if (lines.get(j).contains("<Expression> ::= Identifier")) {
                continue;
            } else if (lines.get(j).contains("<Statements> <Statements>")) {
                continue;
            } else if (lines.get(j).contains("<Statements> ',' <Statements>")) {
                continue;
            } else if (lines.get(j).contains("<Expression> ::= StringLiteral")) {
                continue;
            } else {
                functionBuild = functionBuild + lines.get(j).replace("|", "").replace("+-", "").trim();
            }

            if (lines.get(j).contains(")")) {
                break;
            }
        }
        return functionBuild;
    }
}
