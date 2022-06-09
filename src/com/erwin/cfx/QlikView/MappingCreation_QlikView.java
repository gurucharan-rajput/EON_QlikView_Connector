package com.erwin.cfx.QlikView;

import com.erwin.QlikView.MetadataCreation.MetadataCreation;
import com.ads.api.beans.common.Node;
import com.ads.api.beans.kv.KeyValue;
import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.KeyValueUtil;
import com.ads.api.util.MappingManagerUtil;
import com.ads.api.util.SystemManagerUtil;
import com.erwin.QlikView.MappingCreation.MappingCreatorInfo;
import com.erwin.cfx.connectors.json.syncup.SyncupWithServerDBSchamaSysEnvCPT;
import com.erwin.cfx.QlikView.connectors.test.qlicksense_TreeParsing;
import com.erwin.dataflow.model.xml.snowflake.dataflow;
import com.erwin.sqlparser.snowflake.MappingCreator;
import com.erwin.sqlparser.snowflake.RelationAnalyzer;
import com.erwin.sqlparser.wrapper.parser.snowflake.ErwinSQLWrapper;

import com.icc.util.RequestStatus;
import goldparser.Goldparser;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import java.util.Map;
import java.util.logging.Level;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 *
 * @author DNepak
 */
public class MappingCreation_QlikView {

    MappingCreatorInfo getmap = new MappingCreatorInfo();
    StringBuilder log = new StringBuilder();

    Goldparser gp = new Goldparser();
    qlicksense_TreeParsing qtp = new qlicksense_TreeParsing();
    HashMap<String, String> ExtractTableType = new HashMap<>();
    HashMap<String, String> TransformTableType = new HashMap<>();

    private static final Logger LOGGER = Logger.getLogger(MappingCreation_QlikView.class);
    SystemManagerUtil smUtil;
    MappingManagerUtil mmUtil;
    String sourExtractSQL = "";
    KeyValueUtil keyValueUtil;

    public MappingCreation_QlikView(SystemManagerUtil smUtil, MappingManagerUtil mmUtil, KeyValueUtil keyValueUtil) {
        this.smUtil = smUtil;
        this.mmUtil = mmUtil;
        this.keyValueUtil = keyValueUtil;

    }

    public String createMappingFromFile(String[] sysenvDetails, String projectName, String path, String egtPath, String jsonFilePath, Object defSysName, Object defEnvName) {

        HashMap<Object, Object> inputsMap = new HashMap<>();
        HashMap<String, String> allTablesMap = SyncupWithServerDBSchamaSysEnvCPT.getMap(jsonFilePath, "Tables");
        int projectID = getmap.getProjectId(projectName, mmUtil);
        // int subjectID = getmap.getSubjectId(subjectName, projectID, mmUtil);
        log.append("INPUT");
        log.append("\n\n");
        log.append("Project Name::" + projectName);
        log.append("\n");
        log.append("System Name::" + sysenvDetails[0]);
        log.append("\n");
        log.append("Environment  Name::" + sysenvDetails[1]);
        log.append("\n");
        log.append("Folder Path Provided::" + path);
        log.append("\n\n");

        String mapslist = "";
        inputsMap.put("allTablesMap", allTablesMap);
        inputsMap.put("jsonFilePath", jsonFilePath);

        File filePath = new File(path);

        if (filePath.getName().equals("Export")) {

            mapslist = mappingCreation(filePath.listFiles(), 0, projectID, mapslist, sysenvDetails, inputsMap);
        }
        File[] innerDir = filePath.listFiles();

        for (File file : innerDir) {
            if (file.getName().equals("Export")) {
                mapslist = mappingCreation(file.listFiles(), 0, projectID, mapslist, sysenvDetails, inputsMap);
            } else {
                if (file.isDirectory() && !file.getName().equals("Export")) {
                    mapslist = fileContent(file, projectID, mapslist, sysenvDetails, inputsMap);
                }

            }
        }
       log.append("OUTPUT");
        log.append("\n");

        if (mapslist != "") {
            log.append(mapslist);
            log.append("\n\n");
            log.append("Metadata are Created in System Name::" + sysenvDetails[0]);
            log.append("\n");

        } else {
            log.append("No mapping is created");

        }
        return log.toString();
    }

    private String fileContent(File file, int projectID, String mapslist, String[] sysenvDetails, HashMap<Object, Object> inputsMap) {
        File[] innerDir = file.listFiles();
        int subjectID = getmap.getSubjectId(file.getName(), projectID, mmUtil);
        for (File exportFolder : innerDir) {
            if (exportFolder.getName().equals("Export")) {
                return mappingCreation(exportFolder.listFiles(), subjectID, projectID, mapslist, sysenvDetails, inputsMap);
            }
        }

        return "";
    }

    private String mappingCreation(File[] innerDir, int subjectID, int projectID, String mapslist, String[] sysenvDetails, HashMap<Object, Object> inputsMap) {
        String logData = "";
        for (File logFile : innerDir) {
            String fileContent;
            List<KeyValue> keyValuesList = new ArrayList<>();
            int count = 1;
            try {
                fileContent = FileUtils.readFileToString(logFile, "UTF-8");
                String mapName = logFile.getName();
                mapName = mapName.substring(mapName.lastIndexOf("\\") + 1, mapName.indexOf(".log"));
                mapName = mapName.replaceAll("\\.", "_");
                mapName = mapName.replaceAll("\\+", "");
                Mapping mapping = null;
                String queries[] = fileContent.split("@@Erwin@@");
                ArrayList<MappingSpecificationRow> mappingSpecificationsAdd = new ArrayList<>();
                String sourceExtractQuery = "";
                String extendedProperty = "";
                int mappingId = 0;
                if (subjectID != 0) {
                    mappingId = mmUtil.getMappingId(subjectID, mapName, Node.NodeType.MM_SUBJECT);
                } else {
                    mappingId = mmUtil.getMappingId(projectID, mapName, Node.NodeType.MM_PROJECT);
                }
                if (mappingId == -1) {
                    //create a mpping
                    mapping = new Mapping();
                    mapping.setMappingName(mapName);
                    mapping.setProjectId(projectID);
                    if (subjectID != 0) {
                        mapping.setSubjectId(subjectID);
                    }
                    RequestStatus createMapping = mmUtil.createMapping(mapping);
                    if (createMapping.isRequestSuccess()) {
                        mapslist = mapslist + "\n" + "Map is created ::: " + mapName;
                    } else {
                        mapslist = mapslist + "\n" + "Map is not created ::: " + mapName + " because of error " + createMapping.getStatusMessage().toString();
                        continue;
                    }
                    if (subjectID != 0) {
                        mappingId = mmUtil.getMappingId(subjectID, mapName, Node.NodeType.MM_SUBJECT);
                    } else {
                        mappingId = mmUtil.getMappingId(projectID, mapName, Node.NodeType.MM_PROJECT);
                    }
                } else {
                    mapslist = mapslist + "\n" + "Map is created ::: " + mapName;
                }

                //gett the mapping id
                for (String query : queries) {
                    //query=query.replaceFirst("\n","");
                    if (query.trim().equals("")) {
                        continue;
                    }
                    ArrayList<MappingSpecificationRow> mappingSpecifications;
                    ArrayList<MappingSpecificationRow> mappingSpecifications1 = new ArrayList<>();
                    if (query.contains("##Erwin##")) {
                        String query1 = query.split("##Erwin##")[0];
                        String selectActualTableName = ExtractScriptFromLog_QlikView.actualTableName(query.split("##Erwin##")[0]);
                        selectActualTableName = selectActualTableName.substring(selectActualTableName.lastIndexOf(".") + 1, selectActualTableName.length());
                        selectActualTableName = selectActualTableName.replace("[", "");
                        selectActualTableName = selectActualTableName.replace("]", "");

                        String loadActualTableName = ExtractScriptFromLog_QlikView.actualTableName(query.split("##Erwin##")[1]);
                        loadActualTableName = loadActualTableName.substring(loadActualTableName.lastIndexOf(".") + 1, loadActualTableName.length());
                        loadActualTableName = loadActualTableName.replace("[", "");
                        loadActualTableName = loadActualTableName.replace("]", "");

                        mappingSpecifications = getMappingSpecsFromSQLText(query1, mapName, sysenvDetails);
                        if (mappingSpecifications == null) {
                            logData = logData + "File Name==" + mapName + "\n";
                            logData = logData + "Query==" + query1 + "\n________________________________________\n";
                            continue;
                        }
                        String sourceTable = mappingSpecifications.get(0).getSourceTableName();
                        sourceTable = sourceTable.substring(sourceTable.lastIndexOf(".") + 1, sourceTable.length()).toLowerCase();
                        String targetTable = mappingSpecifications.get(mappingSpecifications.size() - 1).getTargetTableName().toLowerCase();

                        if (StringUtils.containsIgnoreCase(targetTable, "Extract")) {
                            ExtractTableType.put(sourceTable, targetTable);
                        }
                        if (StringUtils.containsIgnoreCase(targetTable, "Transform")) {
                            TransformTableType.put(sourceTable, targetTable);
                        }

                        if (ExtractTableType.containsKey(loadActualTableName) && StringUtils.containsIgnoreCase(mapName, "Transform")) {
                            for (MappingSpecificationRow msrow : mappingSpecifications) {
                                MappingSpecificationRow newmsrow = new MappingSpecificationRow();
                                if (msrow.getSourceTableName().toLowerCase().equals(selectActualTableName.toLowerCase())) {
                                    ExtractTableType.get(loadActualTableName);
                                    newmsrow.setSourceTableName(ExtractTableType.get(loadActualTableName));
                                    newmsrow.setSourceColumnName(msrow.getSourceColumnName());
                                    newmsrow.setTargetTableName(msrow.getSourceTableName());
                                    newmsrow.setTargetColumnName(msrow.getSourceColumnName());
                                    newmsrow.setSourceSystemName(msrow.getSourceSystemName());
                                    newmsrow.setSourceSystemEnvironmentName(msrow.getSourceSystemEnvironmentName());
                                    newmsrow.setTargetSystemName(msrow.getSourceSystemName());
                                    newmsrow.setTargetSystemEnvironmentName(msrow.getSourceSystemEnvironmentName());
                                    mappingSpecifications1.add(newmsrow);
                                }

                            }
                        }

                        if (TransformTableType.containsKey(loadActualTableName) && StringUtils.containsIgnoreCase(mapName, "Dashboard")) {
                            for (MappingSpecificationRow msrow : mappingSpecifications) {
                                MappingSpecificationRow newmsrow = new MappingSpecificationRow();
                                if (msrow.getSourceTableName().toLowerCase().equals(selectActualTableName.toLowerCase())) {
                                    TransformTableType.get(loadActualTableName);
                                    newmsrow.setSourceTableName(TransformTableType.get(loadActualTableName));
                                    newmsrow.setSourceColumnName(msrow.getSourceColumnName());
                                    newmsrow.setTargetTableName(msrow.getSourceTableName());
                                    newmsrow.setTargetColumnName(msrow.getSourceColumnName());
                                    newmsrow.setSourceSystemName(msrow.getSourceSystemName());
                                    newmsrow.setSourceSystemEnvironmentName(msrow.getSourceSystemEnvironmentName());
                                    newmsrow.setTargetSystemName(msrow.getSourceSystemName());
                                    newmsrow.setTargetSystemEnvironmentName(msrow.getSourceSystemEnvironmentName());
                                    mappingSpecifications1.add(newmsrow);
                                }

                            }
                        }

                        if (mappingSpecifications1 != null) {
                            mappingSpecifications.addAll(mappingSpecifications1);
                        }
                        mappingSpecificationsAdd.addAll(mappingSpecifications);

                        if (sourceExtractQuery.isEmpty()) {
                            sourceExtractQuery = query.split("##Erwin##")[1];
                        } else {
                            sourceExtractQuery = sourceExtractQuery + "##@" + query.split("##Erwin##")[1];
                        }
                        extendedProperty = query.split("##Erwin##")[1];

                    } else {
                        mappingSpecifications = getMappingSpecsFromSQLText(query, mapName, sysenvDetails);
                        if (mappingSpecifications == null) {
                            continue;
                        }
                        mappingSpecificationsAdd.addAll(mappingSpecifications);
                        if (sourceExtractQuery.isEmpty()) {
                            sourceExtractQuery = query;
                        } else {
                            sourceExtractQuery = sourceExtractQuery + "##@" + query;
                        }
                        extendedProperty = query.split("##Erwin##")[0];
                    }
                    if (mappingSpecificationsAdd.size() >= 1500) {
                        inputsMap.put("inputSpecsLists", mappingSpecificationsAdd);
                        mmUtil.addMappingSpecifications(mappingId, SyncupWithServerDBSchamaSysEnvCPT.setMetaDataSpec(inputsMap));
                        mappingSpecificationsAdd.clear();
                    }
                    keyValuesList.add(buildKeyValue("LoadScript" + count, extendedProperty));
                    count++;
                }
                if (inputsMap.containsKey("inputSpecsLists")) {
                    inputsMap.remove("inputSpecsLists");

                }
                inputsMap.put("inputSpecsLists", mappingSpecificationsAdd);
                mmUtil.addMappingSpecifications(mappingId, SyncupWithServerDBSchamaSysEnvCPT.setMetaDataSpec(inputsMap));

                //update the project 
                Mapping mapping1 = mmUtil.getMapping(mappingId);
                mapping1.setSourceExtractQuery(sourceExtractQuery);

                try {///create MetaData
                    int sysID = MetadataCreation.createSystem(sysenvDetails[0], smUtil);
                    int sheetEnvId = MetadataCreation.createEnvironmant(sysenvDetails[0], sysenvDetails[1], sysID, smUtil);
                    metadataCreationFromSpec1(mappingSpecificationsAdd, sysID, sheetEnvId);
                    //                   metadataCreationFromSpec(mappingSpecificationsAdd, sysID, sheetEnvId);
                } catch (Exception ex) {
                    System.out.println("Exception :: com.erwin.cfx.QlikView.MappingCreation_QlikView.mappingCreation()");
                    java.util.logging.Logger.getLogger(MappingCreation_QlikView.class.getName()).log(Level.SEVERE, null, ex);
                }

                mmUtil.updateMapping(mapping1);
                keyValueUtil.addKeyValues(keyValuesList, Node.NodeType.MM_MAPPING, mappingId);
            } catch (IOException ex) {
                System.out.println("Exception :: com.erwin.cfx.QlikView.MappingCreation_QlikView.mappingCreation()");
                java.util.logging.Logger.getLogger(MappingCreation_QlikView.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        return mapslist;
    }

    public static KeyValue buildKeyValue(String key, String value) {
        KeyValue kv = new KeyValue(key, value);
        kv.setUIType(KeyValue.UIType.getType("Rich Editor"));
        kv.setType("");
        kv.setPublished(true);
        kv.setVisibility(1);
        return kv;
    }

    private void metadataCreationFromSpec1(ArrayList<MappingSpecificationRow> allQvsMapSpecs, int sysID, int sheetEnvId) {
        String envName = "";
        HashSet<String> sources = new HashSet<>();
        HashMap<String, HashSet<String>> tgtTblAndcols = new HashMap();
        for (MappingSpecificationRow mspecRow : allQvsMapSpecs) {
            envName = mspecRow.getSourceSystemEnvironmentName();
            String[] srcTableNames = mspecRow.getSourceTableName().split("\n");
            for (String srcTableName : srcTableNames) {
                sources.add(srcTableName.toUpperCase().trim());
            }
            String tgtTableName = mspecRow.getTargetTableName().toUpperCase().trim();
            if (tgtTblAndcols.get(tgtTableName) != null) {
                HashSet<String> colms = tgtTblAndcols.get(tgtTableName);
                colms.add(mspecRow.getTargetColumnName().toUpperCase().trim());
                tgtTblAndcols.put(tgtTableName, colms);
            } else {
                HashSet<String> colms = new HashSet<>();
                colms.add(mspecRow.getTargetColumnName().toUpperCase().trim());
                tgtTblAndcols.put(tgtTableName, colms);
            }
        }
        for (String src : sources) {
            if (tgtTblAndcols.keySet().contains(src)) {
                tgtTblAndcols.remove(src);
            }
        }
        int tableID;
        for (Map.Entry<String, HashSet<String>> hm : tgtTblAndcols.entrySet()) {
            String tableName = hm.getKey();
            MetadataCreation.createTable(sheetEnvId, envName, tableName, SMTable.SMTableType.TABLE, smUtil);
            tableID = smUtil.getTableId(sheetEnvId, tableName);
            for (String columnName : hm.getValue()) {
                MetadataCreation.createColumn(smUtil, columnName, tableID, "", "");
            }
        }
    }

    private void metadataCreationFromSpec(ArrayList<MappingSpecificationRow> allQvsMapSpecs, int sysID, int sheetEnvId) {
        // allQvsMapSpecs.get(allQvsMapSpecs.size());

        try {

            for (MappingSpecificationRow specRow : allQvsMapSpecs) {
                int tableID;
                if (specRow != null) {
                    try {
                        String tableName = specRow.getTargetTableName();
                        if (!isAnIntermediateComp(tableName)) {

                            MetadataCreation.createTable(sheetEnvId, specRow.getSourceSystemEnvironmentName(), tableName, SMTable.SMTableType.TABLE, smUtil);
                            tableID = smUtil.getTableId(sheetEnvId, tableName);

                            String columnName = specRow.getTargetColumnName();
                            // String columnDatatype = specRow.getSourceColumnDatatype();

                            MetadataCreation.createColumn(smUtil, columnName, tableID, "", "");
                        }

                    } catch (Exception e) {
                        System.out.println("Exception :: com.erwin.cfx.QlikView.MappingCreation_QlikView.metadataCreationFromSpec()");
                        continue;
                    }

                }
            }
        } catch (Exception ex) {
            System.out.println("Exception :: com.erwin.cfx.QlikView.MappingCreation_QlikView.metadataCreationFromSpec()");
            java.util.logging.Logger.getLogger(MappingCreation_QlikView.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public static boolean isAnIntermediateComp(String name) {
        if (name != null && !name.isEmpty()) {
            name = name.toLowerCase().trim();
            if (name.startsWith("with_cte") || name.startsWith("rs-") || name.startsWith("insert-select")
                    || name.startsWith("update-set") || name.startsWith("merge-update") || name.startsWith("merge-insert") || name.contains("insert-select-")) {
                return true;
            }
        }
        return false;
    }

    private String getFileName(String name) {
        if (name == null || name.lastIndexOf(".") < 0) {
            return "";
        }
        return name.substring(0, name.lastIndexOf("."));
    }

    public ArrayList<MappingSpecificationRow> getMappingSpecsFromSQLText(String sqltext, String fileName, String[] sysenvDetails) {
        ArrayList<MappingSpecificationRow> mapSpecRows = null;
        MappingCreator mc = new MappingCreator();
        ArrayList<MappingSpecificationRow> updatedMapspec = new ArrayList<>();
        try {
            sqltext = ErwinSQLWrapper.removeUnparsedDataFromQuery(sqltext);
            dataflow dtflow = mc.getDataflowFromSql(sqltext, fileName, sysenvDetails);
            if (dtflow == null) {
                return null;
            }
            RelationAnalyzer relationAnalyzer = new RelationAnalyzer();
            mapSpecRows = relationAnalyzer.analyzeRelations(dtflow, sysenvDetails);
            if (mapSpecRows != null && mapSpecRows.size() != 0) {
                for (MappingSpecificationRow msrow : mapSpecRows) {
                    if (msrow.getSourceTableName() == null || msrow.getSourceTableName().trim().length() == 0) {
                        continue;
                    }
                    MappingSpecificationRow updatedmapspecRow = new MappingSpecificationRow();
                    updatedmapspecRow.setSourceSystemName(msrow.getSourceSystemName());
                    updatedmapspecRow.setSourceSystemEnvironmentName(msrow.getSourceSystemEnvironmentName());
                    String tableName = msrow.getSourceTableName().replaceAll("\\[", "").replaceAll("\\]", "");
                    if (tableName.contains(".")) {
                        tableName = tableName.substring(tableName.lastIndexOf(".") + 1, tableName.length());
                    }
                    if (StringUtils.containsIgnoreCase(tableName, "orphan")) {
                        continue;
                    }
                    updatedmapspecRow.setSourceTableName(tableName);
                    updatedmapspecRow.setSourceColumnName(msrow.getSourceColumnName());
                    updatedmapspecRow.setTargetSystemName(msrow.getTargetSystemName());
                    updatedmapspecRow.setTargetSystemEnvironmentName(msrow.getTargetSystemEnvironmentName());
                    tableName = msrow.getTargetTableName().replaceAll("\\[", "").replaceAll("\\]", "");
                    if (tableName.contains("\\.")) {
                        tableName = tableName.substring(tableName.lastIndexOf(".") + 1, tableName.length());
                    }
                    if (StringUtils.containsIgnoreCase(tableName, "orphan")) {
                        continue;
                    }
                    updatedmapspecRow.setTargetTableName(tableName);
                    updatedmapspecRow.setTargetColumnName(msrow.getTargetColumnName());
                    updatedmapspecRow.setBusinessRule(msrow.getBusinessRule());

                    updatedMapspec.add(updatedmapspecRow);
                }
            }
        } catch (Exception ex) {
            System.out.println("Exception :: com.erwin.cfx.QlikView.MappingCreation_QlikView.getMappingSpecsFromSQLText()");
            ex.printStackTrace();
        }
        return updatedMapspec;
    }
}
