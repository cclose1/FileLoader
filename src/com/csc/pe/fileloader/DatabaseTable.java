/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.csc.pe.fileloader;

import org.cbc.utils.data.DatabaseSession;
import org.cbc.utils.system.Logger;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

/**
 *
 * @author CClose
 */
public class DatabaseTable {

    /**
     * @return the removeQuotes
     */
    public boolean isRemoveQuotes() {
        return removeQuotes;
    }

    /**
     * @param removeQuotes the removeQuotes to set
     */
    public void setRemoveQuotes(boolean removeQuotes) {
        this.removeQuotes = removeQuotes;
    }
    public enum FormatType {
        Date(true),
        Time(false),
        Timestamp(true);

        private boolean isDate = false;

        public boolean isDate() {
            return this.isDate;
        }
        private FormatType(boolean isDate) {
            this.isDate = isDate;
        }
    }
    private DatabaseSession  session        = null;
    private Logger           log            = new Logger();
    private String           table          = null;
    private String           schema         = null;
    private ResultSet        insert         = null;
    private boolean          strongType     = true;
    private int              rowCount       = 0;
    private int              firstTimeField = -1;
    private boolean          removeQuotes   = true;

    private class Column {
        String                  name         = null;
        String                  alias        = null;
        String                  defValue     = null;
        String                  inFormat     = null;
        FormatType              formatType   = null;
        Date                    minTime      = null;
        Date                    maxTime      = null;
        String                  translate    = null;
        HashMap<String, String> translations = new HashMap<>(); 
        ResultSet               translator   = null;
        int                     index        = -1;
        int                     type         = -1;

        private Date valueToDate(String value) throws ParseException {
            if (formatType == null || inFormat == null) return null;
            
            Date timestamp;
            
            SimpleDateFormat fmt = new SimpleDateFormat(inFormat);
            fmt.setLenient(false);
            
            timestamp = fmt.parse(value);
            
            if (formatType.isDate) {
                if (minTime == null || minTime.after(timestamp))  minTime = timestamp;
                if (maxTime == null || maxTime.before(timestamp)) maxTime = timestamp;
            }
            return timestamp;
        }
        private void throwColumnError(String message, Exception cause) throws SQLException {
            throw new SQLException(message + " for field " + getQualifiedTable() + "." + name, cause);
        }
        private void throwColumnError(String message) throws SQLException {
            throw new SQLException(message + " for field " + getQualifiedTable() + "." + name);
        }
        Column(String name, String alias) {
            this.name  = name;
            this.alias = alias;
        }
        
        Column(String name) {
            this(name, name);
        }
        void setValue(String value) throws SQLException {
            Date timestamp = null;
            
            if (removeQuotes && value != null) {
                if (value.startsWith("\"")) value = value.substring(1, value.length());
                if (value.endsWith("\""))   value = value.substring(0, value.length() - 1);
            }
            try {
                if (value == null || value.trim().length() == 0 || value.equalsIgnoreCase("null")) {
                    insert.updateNull(index);
                    return;
                } else if (translate != null) {
                    String translated = translations.get(value);
                   
                    try {
                        if (translated == null) {
                            translator = session.executeQuery(
                                    "SELECT * FROM " + translate + " WHERE \"Name\" = '" + value + "'");
                            if (translator.next()) {
                                translated = translator.getString("Value");
                            } else {
                                ResultSet         newValue;
                                PreparedStatement insert   = session.getConnection().prepareStatement(
                                        "INSERT INTO " + translate + " (\"Name\") Values('" + value + "')",
                                        Statement.RETURN_GENERATED_KEYS);
                                insert.executeUpdate();
                                newValue = insert.getGeneratedKeys();
                                
                                if (newValue.next())
                                    translated = newValue.getString("Value");
                                else
                                    throwColumnError("Failed to translate value " + value);
                                
                                newValue.close();
                                insert.close();
                            }
                            translations.put(value, translated);
                            translator.close();
                        }
                    } catch (Exception ex) {                        
                        throwColumnError("Unable to translate " + value + " using translate table " + translate, ex);
                    }
                    value = translated;
                }
                if (strongType) {
                    value = value.trim();

                    switch (type) {
                        case java.sql.Types.INTEGER:
                            insert.updateInt(index, Integer.parseInt(value));
                            break;
                        case java.sql.Types.TIMESTAMP:
                            timestamp = valueToDate(value);
                            insert.updateTimestamp(index, new java.sql.Timestamp(timestamp.getTime()));
                            break;
                        case java.sql.Types.DATE:
                            timestamp = valueToDate(value);
                            insert.updateDate(index, new java.sql.Date(timestamp.getTime()));
                            break;
                        case java.sql.Types.TIME:
                            timestamp = valueToDate(value);
//                            insert.updateString(index, DatabaseSession.getTimeString(timestamp));
                            insert.updateTime(index, new java.sql.Time(timestamp.getTime()));
                            break;
                        case java.sql.Types.DOUBLE:
                            insert.updateDouble(index, Double.parseDouble(value));
                            break;
                        default:
                            insert.updateString(index, value);
                    }
                } else {
                    insert.updateString(index, value);
                }
            } catch (Exception ex) {
                throwColumnError("Unable to convert " + value + " to SQL type " + type, ex);
            }
        }
        java.sql.Timestamp getMinTime() {
            return minTime == null? null : new java.sql.Timestamp(minTime.getTime());
        }
        java.sql.Timestamp getMaxTime() {
            return maxTime == null? null : new java.sql.Timestamp(maxTime.getTime());
        }
    }
    private ArrayList<Column> columns = new ArrayList<>();
    
    public void setSession(DatabaseSession session) {
        this.session = session;
        strongType   = this.session.requiresStrongType();
    }
    public DatabaseSession getSession() {
        return session;
    }
    public void setLog(Logger log) {
        this.log = log;
    }
    public void setTable(String schema, String name) throws SQLException {
        columns.clear();
        this.table  = name;
        this.schema = schema;
        insert = session.updateQuery("SELECT * FROM " + getQualifiedTable() + " WHERE 1 = 0");
    }
    public void setTable(String name) throws SQLException {
        setTable(null, name);
    }
    public String getQualifiedTable(String name) {
        return session.delimitName(schema == null || schema.isEmpty()? name :  schema + "." + name);
    }
    public String getQualifiedTable() {
        return getQualifiedTable(table);
    }
    public String getTable() {
        return table;
    }
    public int getColumnCount() {
        return columns.size();
    }
    /*
     * This is been added to correct the situation where insertRow throws an exception after a
     * successful insert. This is commented on in LoadFile where MySql throws a syntax exception on
     * a successful insert to table WeeklyFuelPrices.
     */
    public void incrementRowCount() {
        rowCount++;
    }
    public int getRowCount() {
        return rowCount;
    }
    public void open() {
        rowCount = 0;

        for (Column column : columns) {
            column.minTime = null;
            column.maxTime = null;
        }
    }
    private int getIndex(String name, boolean isAlias, boolean mustExist) {
        for (int i = 0; i < columns.size(); i++) {
            if (isAlias  && columns.get(i).alias.equals(name)) return i;
            if (!isAlias && columns.get(i).name.equals(name))  return i;
        }
        if (mustExist) {
            
            if (isAlias)
                log.fatalError("Alias " + name + " not enabled for table " + getQualifiedTable());
            else
                log.fatalError("Column " + name + " not enabled for table " + getQualifiedTable());
        }
        return -1;
    }
    public int getColumnIndex(String name) {
        return getIndex(name, false, false);
    }
    public int getAliasIndex(String name) {
        return getIndex(name, true, false);
    }
    public String getAlias(int index) {
        return columns.get(index).alias;
    }
    public int setColumn(String name, String alias) throws SQLException {
        int index = getColumnIndex(name);

        if (index == -1) {
            Column c = new Column(name);

            c.index = insert.findColumn(name);
            c.type  = insert.getMetaData().getColumnType(c.index);
            c.alias = alias;
            columns.add(c);
            index = columns.size() - 1;
        } else
            columns.get(index).alias = alias;

        return index;
    }
    public void setColumns(File file) throws FileNotFoundException, IOException, SQLException {
        BufferedReader cols  = new BufferedReader(new FileReader(file));
        String         line;
        int            count = 0;

        while ((line = cols.readLine()) != null) {
            count++;

            if (!line.trim().isEmpty()) {
                String name  = line;
                String value = null;
                int    i     = line.indexOf('=');

                if (i != -1) {
                    value = line.substring(i + 1).trim();
                    name  = line.substring(0, i).trim();

                    if (value.equalsIgnoreCase("!StartTimestamp")) value = session.getDateTimeString(new Date());
                }
                String fields[] = name.split(",");

                try {
                    switch (fields.length) {
                        case 1:
                            setColumn(fields[0].trim(), fields[0].trim());
                            break;
                        case 2:
                            setColumn(fields[0].trim(), fields[1].trim());
                            break;
                        default:
                            throw new IOException("In column map " + file.getName() + " at line " + count + "- invalid map " + line);
                    }
                } catch (SQLException ex) {
                    throw new SQLException("In column map " + file.getName() + " at line " + count + '-' + ex.getMessage());
                }
                if (value != null) {
                    fields = value.split(",");
                    /*
                     * Note : Does not allow for a value containing a comma. Should be modified to allow this.
                     */
                    for (i = 0; i < fields.length; i++) {
                        if (fields[i].startsWith("!")) {
                            String fmt[] = fields[i].split(":", 2);
                            
                            if (fmt[0].equalsIgnoreCase("!date"))
                                setInputFormat(columns.size() - 1, FormatType.Date, fmt[1]);
                            else if (fmt[0].equalsIgnoreCase("!time"))
                                setInputFormat(columns.size() - 1, FormatType.Time, fmt[1]);
                            else if (fmt[0].equalsIgnoreCase("!translate"))
                                setTranslate(columns.size() - 1, fmt[1]);
                            else
                                throw new SQLException("In column map " + file.getName() + " at line " + count + "- invalid directive " + fmt[1]);
                        } else {
                            setDefault(columns.size() - 1, fields[i]);
                        }
                    }
                }
            }
        }
        cols.close();
    }
    public String getDefault(int index) {
        return columns.get(index).defValue;
    }
    public void setDefault(int index, String value) {
        columns.get(index).defValue = value;
    }
    public void setInputFormat(int index, FormatType type, String value) {
        Column c = columns.get(index);

        c.formatType = type;
        c.inFormat   = value;

        if (type.isDate() && firstTimeField == -1) firstTimeField = index;
    }
    public void setTranslate(int index, String mapTable) {
        Column c = columns.get(index);

        c.translate = mapTable;
    }
    public java.sql.Timestamp getMinTimestamp(int index) {
        return index == -1? null : columns.get(index).getMinTime();
    }
    public java.sql.Timestamp getMaxTimestamp(int index) {
        return index == -1? null : columns.get(index).getMaxTime();
    }
    public java.sql.Timestamp getMinTimestamp() {
        return getMinTimestamp(firstTimeField);
    }
    public java.sql.Timestamp getMaxTimestamp() {
        return getMaxTimestamp(firstTimeField);
    }
    public String getColumnName(int index) {
        return columns.get(index).name;
    }
    public void addRow() throws SQLException {
        insert.moveToInsertRow();
        
        for (Column c : columns) {
            if (c.defValue != null) c.setValue(c.defValue);
        }
    }
    public void setValue(int index, String value) throws SQLException {
        if (index >= columns.size()) log.fatalError("No column with index " + index + " defined for table " + getQualifiedTable());
        
        columns.get(index).setValue(value);
    }
    public void insertRow() throws SQLException {
        insert.insertRow();
        rowCount++;
    }
    public void close() {

    }
}
