// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.analysis;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

/**
 * syntax:
 * STOP JOB FOR [database.]name
 * only run job can be stopped, and stopped job can't be resumed
 */
public class StopJobStmt extends DdlStmt {

    private final LabelName labelName;

    public StopJobStmt(LabelName labelName) {
        this.labelName = labelName;
    }

    public String getName() {
        return labelName.getLabelName();
    }

    public String getDbFullName() {
        return labelName.getDbName();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        checkAuth();
        labelName.analyze(analyzer);
    }

    private void checkAuth() throws AnalysisException {
        UserIdentity userIdentity = ConnectContext.get().getCurrentUserIdentity();
        if (!userIdentity.isRootUser()) {
            throw new AnalysisException("only root user can operate");
        }
    }
}
