/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Stratio
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package com.stratio.connector.irc.engine;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.stratio.connector.irc.manager.IRCManager;
import com.stratio.connector.irc.manager.Message;
import com.stratio.crossdata.common.connector.IQueryEngine;
import com.stratio.crossdata.common.connector.IResultHandler;
import com.stratio.crossdata.common.data.Cell;
import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.ResultSet;
import com.stratio.crossdata.common.data.Row;
import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.LogicalWorkflow;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.ColumnMetadata;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.result.QueryResult;
import com.stratio.crossdata.common.statements.structures.ColumnSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.StringSelector;

/**
 * Skeleton query engine
 */
public class IRCQueryEngine implements IQueryEngine {

    private Map<ClusterName, IRCManager> managers;

    public IRCQueryEngine(Map<ClusterName, IRCManager> managers) {
        this.managers = managers;
    }

    @Override
    public QueryResult execute(LogicalWorkflow workflow) throws ConnectorException {
        ResultSet resultSet = new ResultSet();
        Project project = (Project) workflow.getInitialSteps().iterator().next();
        IRCManager engine = managers.get(project.getClusterName());
        List<Message> messages = engine.getMessagesFromChannel(project.getTableName().getName());
        LogicalStep nextStep = project.getNextStep();
        while (nextStep != null) {
            if (nextStep instanceof Filter) {
                Filter filter = (Filter) nextStep;
                messages = this.applyFilter(messages, filter);
            } else if (nextStep instanceof Select) {
                Select select = (Select) nextStep;
                resultSet = computeResultSet(messages, select);
            }
            nextStep = nextStep.getNextStep();
        }

        return QueryResult.createQueryResult(resultSet);
    }

    @Override
    public void asyncExecute(String queryId, LogicalWorkflow workflow, IResultHandler resultHandler)
            throws ConnectorException {
        throw new UnsupportedException("Method not implemented");
    }

    public ResultSet computeResultSet(List<Message> messages, Select select) {
        ResultSet resultSet = new ResultSet();
        for (Message message : messages) {
            resultSet.add(this.computeRow(message, select));
        }

        //Store the metadata information
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        for (Map.Entry<ColumnName, ColumnType> aliasType : select.getTypeMapFromColumnName().entrySet()) {
            ColumnName columnName = aliasType.getKey();
            columnName.setAlias(select.getColumnMap().get(columnName));
            ColumnMetadata metadata = new ColumnMetadata(columnName, null, aliasType.getValue());
            columnMetadataList.add(metadata);
        }
        resultSet.setColumnMetadata(columnMetadataList);
        return resultSet;
    }

    public Row computeRow(Message message, Select select) {
        Row result = new Row();
        for (ColumnName columnName : select.getColumnMap().keySet()) {
            String alias = select.getColumnMap().get(columnName);
            switch (columnName.getName()) {
            case "message":
                result.addCell(alias, new Cell(message.getMessage()));
                break;
            case "timestamp":
                result.addCell(alias, new Cell(message.getTimestamp()));
                break;
            case "host":
                result.addCell(alias, new Cell(message.getHost()));
                break;
            case "channel":
                result.addCell(alias, new Cell(message.getChannel()));
                break;
            default:
                result.addCell(alias, new Cell(""));
            }
        }
        return result;
    }

    private List<Message> applyFilter(List<Message> messages, Filter filter) throws UnsupportedException {
        List<Message> result = new ArrayList<Message>();
        for (Message message : messages) {
            if(this.applyFilter(message, filter)) {
                result.add(message);
            }
        }
        return result;
    }

    private boolean applyFilter(Message message, Filter filter) throws UnsupportedException {
        boolean result = false;
        ColumnSelector columnSelector = (ColumnSelector) filter.getRelation().getLeftTerm();
        StringSelector stringSelector;
        if (filter.getRelation().getRightTerm() instanceof StringSelector) {
            stringSelector = (StringSelector) filter.getRelation().getRightTerm();
        } else {
            throw new UnsupportedException("Only support String comparator");
        }
        switch (columnSelector.getName().getName()) {
        case "message":
            result = applyFilter(message.getMessage(), filter.getRelation().getOperator(), stringSelector);
            break;
        case "timestamp":
            result = applyFilter(message.getTimestamp(), filter.getRelation().getOperator(), stringSelector);
            break;
        case "host":
            result = applyFilter(message.getHost(), filter.getRelation().getOperator(), stringSelector);
            break;
        case "channel":
            result = applyFilter(message.getChannel(), filter.getRelation().getOperator(), stringSelector);
            break;
        }
        return result;
    }

    private boolean applyFilter(String value, Operator operator, StringSelector selector) throws UnsupportedException {
        boolean result = false;
        switch (operator) {
        case EQ:
            result = value.equals(selector.getStringValue());
            break;
        case MATCH:
            result = value.matches("(.*)" + selector.getStringValue() + "(.*)");
            break;
        case GT:
            result = value.compareTo(selector.getStringValue()) > 0;
            break;
        case LT:
            result = value.compareTo(selector.getStringValue()) < 0;
            break;
        case GET:
            result = value.compareTo(selector.getStringValue()) >= 0;
            break;
        case LET:
            result = value.compareTo(selector.getStringValue()) <= 0;
            break;
        case DISTINCT:
            result = !value.equals(selector.getStringValue());
            break;
        default:
            throw new UnsupportedException("Operator not supported");
        }
        return result;
    }

    @Override
    public void stop(String queryId) throws ConnectorException {
        throw new UnsupportedException("Method not implemented");
    }
}
