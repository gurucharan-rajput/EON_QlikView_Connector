/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.erwin.QlikView.MappingCreation;

import com.ads.api.beans.mm.Mapping;
import com.ads.api.beans.mm.MappingSpecificationRow;
import com.ads.api.beans.mm.Project;
import com.ads.api.util.MappingManagerUtil;
import com.icc.util.RequestStatus;
import java.util.ArrayList;

/**
 *
 * @author DNepak
 */
public class MappingCreatorInfo {
    public int getProjectId(String projectName, MappingManagerUtil mmutil) {
        int projectId = -1;
        try {
            projectId = mmutil.getProjectId(projectName);

        } catch (Exception e) {
            e.printStackTrace();
        }
        if (projectId < 1) {
            Project project = new Project();
            project.setProjectName(projectName);
            RequestStatus requestStatus = mmutil.createProject(project);
            try {
                projectId = mmutil.getProjectId(projectName);

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return projectId;
    }
    
     public int getSubjectId(String subjectName, int projectId, MappingManagerUtil mmUtil) {
        int subjectId = 0;
        try {
            Project project = mmUtil.getProject(projectId);
            subjectId = mmUtil.getSubjectId(project.getProjectName(), subjectName);
            if (subjectId <= 0) {
                RequestStatus<?> subReq = mmUtil.createSubject(projectId, -1, subjectName);
                if (subReq != null && subReq.isRequestSuccess()) {
                    subjectId = Integer.parseInt(subReq.getUserObject().toString());
                }
            }
        } catch (Exception e) {
          
        }
        return subjectId;
    }
     
     public Mapping createMapping(ArrayList<MappingSpecificationRow> mapSpecRows,int projectId,int subjectId,String mapname,String sqltext){
        
         Mapping mapping=new Mapping();
         mapping.setMappingName(mapname);
            mapping.setProjectId(projectId);
            mapping.setSubjectId(subjectId);
            mapping.setMappingSpecifications(mapSpecRows);
            mapping.setSourceExtractQuery(sqltext);
        return mapping;
       
         
     }
    
}
