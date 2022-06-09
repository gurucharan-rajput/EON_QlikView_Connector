/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.QlikView.MetadataCreation;

import com.ads.api.beans.sm.SMColumn;
import com.ads.api.beans.sm.SMEnvironment;
import com.ads.api.beans.sm.SMSystem;
import com.ads.api.beans.sm.SMTable;
import com.ads.api.util.SystemManagerUtil;
import com.icc.util.RequestStatus;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author DeepakNepak
 */
public class MetadataCreation {
    
    public static int createSystem(String sysName, SystemManagerUtil SMUtil){
        
        int sysId = SMUtil.getSystemId(sysName);
//        if (sysId>0){
//            SMSystem sys = SMUtil.getSystem(sysName);
//            ArrayList<SMEnvironment> envList=SMUtil.getEnvironments(sys.getSystemId());
//       
//            for(int i=0 ; i< envList.size(); i++){
//                SMEnvironment env=envList.get(i);
//                SMUtil.deleteEnvironment(env.getEnvironmentId());
//            }
//            SMUtil.deleteSystem(sys.getSystemId());
//        }
        SMSystem sysDetails=new SMSystem();
        sysDetails.setSystemName(sysName);
        SMUtil.createSystem(sysDetails);
        sysId=SMUtil.getSystemId(sysName);
        
        return sysId;        
    }

    public static int createEnvironmant(String sysName, String envName, int sysID, SystemManagerUtil SMUtil) throws Exception {

        //Creating Envormant 
        SMEnvironment env = new SMEnvironment();
        env.setSystemEnvironmentName(envName);
        env.setDatabaseType("sqlServer");
        env.setSystemId(sysID);
        env.setNoOfPartitions(1);
        env.setMaximumNoOfConnectionsPerPartition(5);
        env.setMinimumNoOfConnectionsPerPartition(3);
        env.setDatabaseUserName("sa");
        env.setDatabasePassword("goerwin@1");
        env.setDatabaseURL("jdbc:sqlserver://localhost:1433;databaseName=xe");
        env.setDatabaseDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        //  env.setDatabaseDomain(dBDomain);
        env.setDatabaseIPAddress("localhost");
        env.setDatabasePort("1433");
        env.setDatabaseName("xe");
        RequestStatus rs = null;
        try {
            rs = SMUtil.createEnvironment(env);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return SMUtil.getEnvironmentId(sysName, envName);
    }//create Environment

    public static SMTable createTable(int envID, String envName, String tableName, SMTable.SMTableType tableType, SystemManagerUtil SMUtil) {

        SMTable SMTable = new SMTable();
        //Craetion of table
        SMTable.setEnvironmentId(envID);
        SMTable.setSystemEnvironmentName(envName);
//        SMTable.setSchemaName(schema);
        SMTable.setTableName(tableName);
        SMTable.setTableReferenceId(tableName);
        SMTable.setTableType(tableType);
        SMTable.setTableDefinition(tableName + "_Table");
        RequestStatus rs = SMUtil.createTable(SMTable);
        return SMTable;
    }//create table

    public static String createColumn(SystemManagerUtil SMUtil, String columnname, int tableId,String foreignColumnName,String foreignTableName  ) {
        RequestStatus rc = null;
        SMColumn sMColumn = new SMColumn();
        sMColumn.setColumnName(columnname);
        sMColumn.setTableId(tableId);
        sMColumn.setColumnType(SMColumn.SMColumnType.ENTITY_ELEMENT);
        if(foreignColumnName!="" && foreignColumnName!=null && foreignTableName!="" && foreignTableName!=null){
            sMColumn.setForeignKeyColumnName(foreignColumnName);
            sMColumn.setForeignKeyTableName(foreignTableName);
        }
        rc = SMUtil.createColumn(sMColumn);
        return rc.getStatusMessage();
    }

}
