/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.cfx.QlikView.connectors.test;

/**
 *
 * @author PSurya
 */
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.util.MappingManagerUtil;
import com.icc.util.RequestStatus;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

public class qlicksense_TreeParsing {

	public static void main(String[] args) throws IOException {
            /*
		String tempColumn = "";
		List<String> columnName = new ArrayList();
		List<String> lines = FileUtils.readLines(new File("D:\\My WorkSpace\\QLIK_Sense\\QlickSenseNew\\a1.txt"), "UTF-8");
		for (int i = 0; i < lines.size(); i++) {
			if (lines.get(i).contains("<MyFun> ::=")) {
				System.out.println(i);
				String temp = "";
				for (int j = i; j < lines.size(); j++) {
					if (lines.get(j).contains("<Statement> ::= '[' <Statements> ']'")) {
						continue;
					} else if (lines.get(j).contains("as")) {
						continue;
					} else if (lines.get(j).contains("<Expression> ::= Identifier")) {
						if (lines.get(j + 2).contains("&")) {
							temp = temp + " " + lines.get(j + 1).replace("|", "").replace("+-", "").trim() + ",";
						} else if (lines.get(j + 2).contains("_")) {
							temp = temp + "" + lines.get(j + 1).replace("|", "").replace("+-", "").trim() + "_".trim();
						} else {
							temp = temp + " " + lines.get(j + 1).replace("|", "").replace("+-", "").trim();
						}
					} else if (lines.get(j).contains("<Expression> ::= StringLiteral")) {
						temp = lines.get(j + 1).replace("|", "").replace("+-", "").trim();

					} else if (lines.get(j).contains("]")) {
						i = j;
						columnName.add(temp);
						temp = "";
					}

				}
			}
		}
		System.out.println(columnName);
		List<String> columnName1 = new ArrayList();
		for (String aa : columnName) {
			columnName1.add(aa.replace("as", "").replace("FROM_", ""));

		}
		

		String tableName = columnName1.get(columnName1.size() - 1);
		columnName1.remove(columnName1.size() - 1);
		System.out.println(tableName);
		System.out.println(columnName1);
		
		Map<String,List<String>> hm = new HashMap();
		hm.put(tableName, columnName1);
		System.out.println(hm);
	*/
            String tempColumn = "";
		List<String> columnName = new ArrayList();
		List<String> lines = FileUtils.readLines(new File("C:\\Users\\DNepak\\Downloads\\a2.txt"), "UTF-8");
		for (int i = 0; i < lines.size(); i++) {
			if (lines.get(i).contains("<MyFun> ::=")) {
				System.out.println(i);
				String temp = "";
				for (int j = i; j < lines.size(); j++) {
					if (lines.get(j).contains("<Statement> ::= '[' <Statements> ']'")) {
						continue;
					} else if (lines.get(j).contains("as")) {
						continue;
					} else if (lines.get(j).contains("<Expression> ::= Identifier")) {
						if (lines.get(j + 2).contains("&")) {
							temp = temp + " " + lines.get(j + 1).replace("|", "").replace("+-", "").trim() + ",";
						} else if (lines.get(j + 2).contains("_")) {
							temp = temp + "" + lines.get(j + 1).replace("|", "").replace("+-", "").trim() + "_".trim();
						} else {
							temp = temp + " " + lines.get(j + 1).replace("|", "").replace("+-", "").trim();
						}
					} else if (lines.get(j).contains("<Expression> ::= StringLiteral")) {
						temp = lines.get(j + 1).replace("|", "").replace("+-", "").trim();

					} else if (lines.get(j).contains("]")) {
						i = j;
						columnName.add(temp);
						temp = "";
					}

				}
			}
		}
		System.out.println(columnName);
		List<String> columnName1 = new ArrayList();
		for (String aa : columnName) {
			columnName1.add(aa.replace("as", "").replace("FROM_", ""));

		}
		

		String tableName = columnName1.get(columnName1.size() - 1);
		columnName1.remove(columnName1.size() - 1);
		System.out.println(tableName);
		System.out.println(columnName1);
		
		Map<String,List<String>> hm = new HashMap();
		hm.put(tableName, columnName1);
		System.out.println(hm);
	}
        
        public Map<String,List<String>> getTableInfo(String qlikTree){
            String tempColumn = "";
		List<String> columnName = new ArrayList();
		String[] lines = qlikTree.split("\n");
		for (int i = 0; i < lines.length; i++) {
			if (lines[i].contains("<MyFun> ::=")) {
				System.out.println(i);
				String temp = "";
				for (int j = i; j < lines.length; j++) {
					if (lines[j].contains("<Statement> ::= '[' <Statements> ']'")) {
						continue;
					} else if (lines[j].contains("as")) {
						continue;
					} else if (lines[j].contains("<Expression> ::= Identifier")) {
						if (lines[j + 2].contains("&")) {
							temp = temp + " " + lines[j + 1].replace("|", "").replace("+-", "").trim() + ",";
						} else if (lines[j + 2].contains("_")) {
							temp = temp + "" + lines[j + 1].replace("|", "").replace("+-", "").trim() + "_".trim();
						} else {
							temp = temp + " " + lines[j + 1].replace("|", "").replace("+-", "").trim();
						}
					} else if (lines[j].contains("<Expression> ::= StringLiteral")) {
						temp = lines[j + 1].replace("|", "").replace("+-", "").trim();

					} else if (lines[j].contains("]")) {
						i = j;
						columnName.add(temp);
						temp = "";
					}

				}
			}
		}
		System.out.println(columnName);
		List<String> columnName1 = new ArrayList();
		for (String aa : columnName) {
			columnName1.add(aa.replace("as", "").replace("FROM_", ""));

		}
		

		String tableName = columnName1.get(columnName1.size() - 1);
		columnName1.remove(columnName1.size() - 1);
		System.out.println(tableName);
		System.out.println(columnName1);
		
		Map<String,List<String>> hm = new HashMap();
		hm.put(tableName, columnName1);
	//	System.out.println(hm);
            return hm;
        }
        
   public String parseQlickTree(String filePath,MappingManagerUtil maputil,int projcetId,String mapName){
            try {
                String tempColumn = "";
		List<String> columnName = new ArrayList();
		List<String> lines = FileUtils.readLines(new File(filePath), "UTF-8");
		for (int i = 0; i < lines.size(); i++) {
			if (lines.get(i).contains("<MyFun> ::=")) {
				System.out.println(i);
				String temp = "";
				for (int j = i; j < lines.size(); j++) {
					if (lines.get(j).contains("<Statement> ::= '[' <Statements> ']'")) {
						continue;
					} else if (lines.get(j).contains("as")) {
						continue;
					} else if (lines.get(j).contains("<Expression> ::= Identifier")) {
						if (lines.get(j + 2).contains("&")) {
							temp = temp + " " + lines.get(j + 1).replace("|", "").replace("+-", "").trim() + ",";
						} else if (lines.get(j + 2).contains("_")) {
							temp = temp + "" + lines.get(j + 1).replace("|", "").replace("+-", "").trim() + "_".trim();
						} else {
							temp = temp + " " + lines.get(j + 1).replace("|", "").replace("+-", "").trim();
						}
					} else if (lines.get(j).contains("<Expression> ::= StringLiteral")) {
						temp = lines.get(j + 1).replace("|", "").replace("+-", "").trim();

					} else if (lines.get(j).contains("]")) {
						i = j;
						columnName.add(temp);
						temp = "";
					}

				}
			}
		}
		System.out.println(columnName);
		List<String> columnName1 = new ArrayList();
		for (String aa : columnName) {
			columnName1.add(aa.replace("as", "").replace("FROM_", ""));

		}
		

		String tableName = columnName1.get(columnName1.size() - 1);
		columnName1.remove(columnName1.size() - 1);
		System.out.println(tableName);
		System.out.println(columnName1);
		
		Map<String,List<String>> hm = new HashMap();
		hm.put(tableName, columnName1);
		System.out.println(hm);
                //String msg=createMappingForQlick(hm,"",maputil,projcetId,sumapName);
                return null;
            } catch (IOException ex) {
                ex.printStackTrace();
                Logger.getLogger(qlicksense_TreeParsing.class.getName()).log(Level.SEVERE, null, ex);
            }
            return  null;
   }

    public String  createMappingForQlick(Map<String, List<String>> hm,String targetTableName, MappingManagerUtil maputil, int projcetId,int subID, String mapName,String sourceExtract) {
        try{
            Mapping map=new Mapping();
            String srctabName=getSourceTable(hm);
            MappingSpecificationRow mapRow=null;
            ArrayList<MappingSpecificationRow> storeMapSpecRow=new ArrayList<>();
            for (Map.Entry<String,List<String>> entry : hm.entrySet()){
                
           if(entry.getKey().equalsIgnoreCase("functions")){
               continue;
               //mapRow.setBusinessRule(colName);
           }
           List<String> colDetails=entry.getValue();
           colDetails.remove("LOAD");
            colDetails.remove("Load");
           colDetails.remove("where");colDetails.remove("and");colDetails.remove("-");
           colDetails.remove("DISTINCT");
            colDetails.remove("Len");
           for(String colName:colDetails){
               
                    mapRow=new MappingSpecificationRow();
                StringBuilder storeColDetails=new StringBuilder();
           String srcTableName=entry.getKey();
           mapRow.setSourceSystemName("SYS");
           mapRow.setSourceSystemEnvironmentName("ENV");
           
           mapRow.setSourceTableName(srctabName);
           mapRow.setTargetSystemName("SYS");
           mapRow.setTargetSystemEnvironmentName("ENV");
           mapRow.setTargetTableName(targetTableName);
            StringUtils.removeIgnoreCase(colName, "LOAD");
            StringUtils.removeIgnoreCase(colName, "DISTINCT");
           mapRow.setSourceColumnName(colName.replace('"', ' ').replaceAll(",","").replace("'", ""));
           mapRow.setTargetColumnName(colName.replace('"', ' ').replaceAll(",","").replace("'", ""));
               System.out.println("TGT COL NAME="+colName.replace('"', ' ').replaceAll(",","").replace("'", ""));
               storeMapSpecRow.add(mapRow);
               
//               storeColDetails.append(colName.replace('"', ' ').replaceAll(",","").replace("'", ""));
//               storeColDetails.append("\n");
           }
           
           
        }
            map.setSourceExtractQuery(sourceExtract);
            map.setMappingName(srctabName);
            map.setProjectId(projcetId);
            map.setSubjectId(subID);
            map.setMappingSpecifications(storeMapSpecRow);
            RequestStatus meg=maputil.createMapping(map);
            return meg.getStatusMessage();
        }catch(Exception e){
            e.printStackTrace();
        }
        return  null;
    }
    
    public String getSourceTable(Map<String, List<String>> hm){
        try{
            String srctabName="";
            for (Map.Entry<String,List<String>> entry : hm.entrySet()){
            
                if(!entry.getKey().equalsIgnoreCase("functions")){
                    srctabName=entry.getKey();
                    return srctabName;
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return null;
        
    }
}
