/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.csc.pe.fileloader;

import org.cbc.utils.data.DatabaseSession;
import org.cbc.utils.system.Logger;
import org.cbc.utils.system.Timer;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

/**
 *
 * @author CClose
 */
public class LoadData  {
    private Logger             log     = new Logger();
    private String             loadLog = null;
    private ArrayList<Integer> index   = new ArrayList<>();

    public String getMessageWithCause(SQLException error) {
        return error.getMessage() + (error.getCause() != null? " cause-" + error.getCause().getMessage() : "");
    }
    public void setLog(Logger log) {
        this.log = log;
    }
    public void setLoadLog(String loadLog) {
        this.loadLog = loadLog;
    }
    private java.sql.Timestamp getSQLTimestamp(Date timestamp) {
        return timestamp == null? null : new java.sql.Timestamp(timestamp.getTime());
    }
    private class LoadLog {
        private DatabaseSession session = null;
        private String          table   = null;
        private String          type       = null;
        private String          reference  = null;
        private boolean         currentLog = false;
        private ResultSet       log        = null;
        private String          sql        = null;
        private boolean         isNew      = false;
        private boolean         flush      = false;

        private boolean openResultSet() throws SQLException {
            if (loadLog == null) return false;

            if (!currentLog) {
                log = session.updateQuery(sql);
                
                if (isNew = !log.next()) {
                    log.moveToInsertRow();
                    log.updateString("Type",      type);
                    log.updateString("Reference", reference);
                    log.updateString("Table",     table);
                    log.updateTimestamp("Loaded", getSQLTimestamp(new Date()));
                }
                currentLog = true;
                flush      = false;
            }
            return true;
        }
        private boolean openResultSet(boolean forUpdate) throws SQLException {
            boolean isAvailable = openResultSet();

            if (isAvailable && forUpdate) flush = true;

            return isAvailable;
        }
        public void open(DatabaseSession session, String table, String type, String reference) throws SQLException {
            if (loadLog == null) return;

            this.session   = session;
            this.table     = table;
            this.type      = type;
            this.reference = reference;
            sql = "SELECT * FROM " + loadLog  + " WHERE Type = '" + type + "' AND Reference = '" + reference + "'";
            openResultSet();
        }
        public LoadLog() {

        }
        public LoadLog(DatabaseSession session, String table, String type, String reference) throws SQLException {
            open(session, table, type, reference);
        }
        public boolean isNew() {
            return this.isNew;
        }
        public java.sql.Date getLoaded() throws SQLException {
            if (openResultSet(true)) return log.getDate("Loaded");

            return null;
        }
        public void setRows(int rows) throws SQLException {
            if (openResultSet(true)) log.updateInt("Rows", rows);
        }
        public void setDataStart(java.sql.Timestamp start) throws SQLException {
            if (openResultSet(true)) log.updateTimestamp("DataStart", start);
        }
        public void setDataEnd(java.sql.Timestamp end) throws SQLException {
            if (openResultSet(true)) log.updateTimestamp("DataEnd", end);
        }
        public void setDuplicates(int duplicates) throws SQLException {
            if (openResultSet(true)) log.updateInt("Duplicates", duplicates);
        }
        public void setErrors(int duplicates) throws SQLException {
            if (openResultSet(true)) log.updateInt("Errors", duplicates);
        }
        public void setDuration(double duration) throws SQLException {
            if (openResultSet(true)) log.updateDouble("Duration", duration);
        }
        public void flush() throws SQLException {
            if (openResultSet()) {
                if (this.flush || isNew) {
                    if (isNew)
                        log.insertRow();
                    else
                        log.updateRow();
                }
            }
            currentLog = false;
        }
        public void close() throws SQLException {
            flush();
        }
        public void close(boolean flush) throws SQLException {
            if (flush) this.flush();
        }
    }
    public boolean isLoaded(DatabaseTable table, String dataType, String dataReference, boolean log) throws SQLException {
        LoadLog dbLog = new LoadLog(table.getSession(), table.getTable(), dataType, dataReference);
        
        boolean loaded = !dbLog.isNew();

        dbLog.close(false);

        if (loaded && log)
            this.log.report(
                    Logger.Type.Warning,
                    "Data set " + dataReference + " already loaded on " + dbLog.getLoaded());

        return loaded;
    }
    private void loadData(DataSource data, DatabaseTable table, boolean allowExists) throws SQLException, IOException {
        ArrayList<String> values;
        Timer             timer                 = new Timer();
        int               duplicates            = 0;
        int               errors                = 0;
        boolean           spuriousErrorReported = false;
        LoadLog           dbLog                 = new LoadLog(table.getSession(), table.getTable(), data.getType(), data.getReference());

        if (!dbLog.isNew()) {
            this.log.report(
                    Logger.Type.Warning,
                    "Data set " + data.getReference() + " already loaded on " + dbLog.getLoaded());
            return;
        }
        table.open();

        try {
            while ((values = data.getValues()) != null) {
                try {
                    table.addRow();

                    for (int i = 0; i < values.size(); i++) {
                        int fldIndex = index.get(i);

                        if (fldIndex != -1) {
                            table.setValue(fldIndex, values.get(i));
                        }
                    }
                    table.insertRow();
                } catch (SQLException ex) {
                    if (allowExists && table.getSession().getStandardError(ex) == DatabaseSession.Error.Duplicate) {
                        duplicates++;
                        log.warning("Data at " + data.getLocation() + " already loaded to " + table.getTable());
                    } else {
                        if (ex.getErrorCode() == 1064 && table.getSession().getProtocol().equalsIgnoreCase("mysql")) {
                            /*
                             * On mysql the data insert appears to work, but an exception indicating a syntax error
                             * is thrown. Report the first occurrence of it. So far only occurs on table WeeklyFuelPrices.
                             *
                             * Note: This was not always the case. Don't know what changed to cause this.
                             */
                            if (!spuriousErrorReported) {
                                log.warning("Data at " + data.getLocation() + " generated syntax error, although loaded. Subsequent such errors ignored");
                                log.warning("Syntax error messsage-" + ex.getMessage());
                                spuriousErrorReported = true;
                            }
                            table.incrementRowCount();
                        } else {
                            errors++;
                            log.error("At " + data.getLocation() + " error-" + getMessageWithCause(ex));
                        }
                    }
                }
            }
        } catch (IOException ex) {
            errors++;
            log.error("At " + data.getLocation() + " error-" + ex.getMessage());
        }
        table.close();
        dbLog.setRows(table.getRowCount());
        dbLog.setDataStart(table.getMinTimestamp());
        dbLog.setDataEnd(table.getMaxTimestamp());
        dbLog.setDuplicates(duplicates);
        dbLog.setErrors(errors);
        dbLog.setDuration(timer.getElapsed());
        dbLog.close();
    }
    
    private void logMapping(String dataColumn, String dbColumn, boolean first) {
        StringBuilder line = new StringBuilder();

        line.append(dataColumn);

        if (first) logMapping("Data Column", "Mapped", false);
        if (dbColumn != null) {
            while (line.length() <= 30) line.append(' ');
            
            line.append(dbColumn);
        }
        log.comment(line.toString());
    }
    public void load(DataSource data, DatabaseTable table, boolean allowExists, boolean reportMapping) throws SQLException, IOException {
        int count = table.getColumnCount();
        
        for (int i = 0; i < count; i++) {
            if (!data.getColumns().contains(table.getAlias(i)) && table.getDefault(i) == null) 
                throw new IOException("Data does not contain field " + table.getAlias(i) + " mapped to " + table.getColumnName(i) + " in table " + table.getQualifiedTable());
        }
        /*
         * If no columns have been selected, add all the available data column, otherwise we add only the data columns thst
         * have an alias defined in data. The array list index will have a column index of -1 for those data columns that have
         * not been assigned to a table column.
         *
         */
        for (String column : data.getColumns()) {
            int col = count == 0? table.setColumn(column, column) : table.getAliasIndex(column);

            if (reportMapping) logMapping(column, col == -1? null : table.getColumnName(col), index.isEmpty());

            index.add(col);
        }
        loadData(data, table, allowExists);
    }
}
