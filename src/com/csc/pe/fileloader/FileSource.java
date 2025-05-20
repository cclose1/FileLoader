/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.csc.pe.fileloader;

import org.cbc.utils.system.Logger;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

/**
 *
 * @author cclose
 */
public class FileSource implements DataSource {
    protected ArrayList<String>  columns          = null;
    protected ArrayList<Integer> filterIndex      = null;
    protected Logger             log              = new Logger();
    protected String             reference        = null;
    protected String             type             = "FileTable";
    private   InputStream        stream           = null;
    protected BufferedReader     reader           = null;
    protected int                count            = 0;
    protected char               fieldDelimiter   = ',';
    protected SourceFilter       filter           = null;
    protected boolean            hasHeader        = true;
    protected String             lastLine         = null;
    protected String             lastKeyColumn    = null;
    protected String             pivotKeyColumn   = null;
    protected String             pivotValueColumn = null;
    protected String             tagColumn        = null;
    protected String             tagSeparator     = null;
    protected int                pivotFirst       = -1;
    protected int                pivotIndex       = 0;
    private   ArrayList<String>  currentValues    = null;
    private   ArrayList<String>  tagFields        = null;
    private   int                tagIndex         = -1;

    /*
     * There is a TagMap entry for each of the source data columns in a tagged data set.
     *
     * tag is the index in tags of the tag value which provides the value for the tagColum. A value of -1
     * indicates that the value is not tagged and is added to all rows derived from the source data.
     *
     * index is the column index. The tagColumn is the first column and has index = 0.
     */
    private class TagMap {
        int tag;
        int index;
    }
    /*
     * A data row is generated from the source data for each row in tags.
     */
    private ArrayList<String> tags       = new ArrayList<String>();
    private ArrayList<TagMap> tagColumns = new ArrayList<TagMap>();

    private void setTagMap() {
        tagIndex  = 0;
        tagFields = null;

        if (tagColumn == null) return;

        ArrayList<String> cols = new ArrayList<String>();
        
        cols.add(tagColumn);
        
        for (int i = 0; i < columns.size(); i++) {
            TagMap map = new TagMap();
            
            String tagVal[] = columns.get(i).split(tagSeparator, 2);
            String colName  = null;
            
            if (tagVal.length == 2) {
                // We have a tagged field. Add it to the tags if not already there.
                
                if (!tags.contains(tagVal[0])) tags.add(tagVal[0]);
                
                map.tag = tags.indexOf(tagVal[0]);
                colName = tagVal[1];
            } else {
                map.tag = -1;
                colName = tagVal[0];
            }
           
            // Add column name if not already in the list.
            
            if (!cols.contains(colName)) cols.add(colName);

            map.index = cols.indexOf(colName);
            tagColumns.add(map);
        }
        columns = cols;
    }
    /*
     * If tagIndex points to a valid entry, the fields matching data are returned and tagIndex is incremented.
     */
    private ArrayList<String> getTagFields() {
        ArrayList<String> fields = null;

        if (tagColumn == null || tagIndex >= tags.size()) return fields;
        fields = new ArrayList<>(columns.size());

        fields.add(0, tags.get(tagIndex));

        for (int i = 0; i < tagColumns.size(); i++) {
            TagMap map = tagColumns.get(i);

            if (map.tag == -1 || map.tag == tagIndex) fields.add(map.index, tagFields.get(i));
        }
        tagIndex += 1;

        return fields;
    }
    private void setFilter() {
        if (columns != null) {
            filterIndex = new ArrayList<>();

            if (filter != null) {
                for (int i = 0; i < columns.size(); i++) {
                    int index = filter.getIndex(columns.get(i));

                    filterIndex.add(index == -1? null : index);
                }
            }
        }
    }
    protected void applyPivot() throws IOException {
        if (pivotKeyColumn != null) {
            pivotFirst = columns.indexOf(lastKeyColumn);
            
            if (pivotFirst == -1) throw new IOException("In applying pivot the last key column " + lastKeyColumn + " not in columns");
            
            pivotFirst++;
        }
    }
    /**
     * Need to change this to a custom split so that field parsing is more general, e.g. to handle a space separator. In the
     * case of the standard split consecutive spaces will create separate fields, whereas you would normally
     * expect consecutive space an tabs to be treated as a single separator
     *<p>
     * Returns an array Strings for the fields in line delimited by fieldSeparator. If minLength is not zero the
     * array is extend by adding null strings until the size is at least minSize. Empty fields are set to null.
     *
     * @param line a string of delimited fields
     * @param minLength if none zero the minimum size of fields array
     * @return fields array or null if line is null
     *
     */
    protected ArrayList<String> unpack(String line, int minLength) {
        if (line == null) return null;      
        
        ArrayList<String> fields = new ArrayList<>(Arrays.asList(line.split("" + fieldDelimiter)));
        /*
         * Remove leading and trailing spaces and set empty fields to null.
         */
        for (int i = 0; i < fields.size(); i++) {
            String s = fields.get(i).trim();
            
            if (s.isEmpty()) s = null;
            
            fields.set(i, s);
        }
        while (fields.size() < minLength) fields.add(null);

        return fields;
    }
    protected void createColumns(String headings) {
        columns = unpack(headings, 0);
        setFilter();
    }
    protected void createColumns(int count) {
        columns = new ArrayList<>(count);

        for (int i = 1; i <= count; i++) columns.add("Col" + i);

        setFilter();
    }
    public void setFieldDelimiter(char delimiter) {
        fieldDelimiter = delimiter;
    }
    protected void throwIOError(String message) throws IOException {
        throw new IOException("At " + getLocation() + '-' + message);
    }
    /**
     * Returns the next none blank line from the input stream. Count is incremented for each line read from the
     * input stream, i.e. the count includes blank lines.
     *<p>
     * @return Next none blank line or null if the input stream is empty.
     *
     * @throws IOException
     */
    protected String getLine() throws IOException {
        String line = null;

        if (reader == null) throw new IOException("There is no open input stream");

        while ((line == null? line = reader.readLine() : line) != null) {
            count += 1;
            
            if (!line.trim().isEmpty()) return line;

            line = null;
        }
        return null;
    }
    private boolean filterExcludes(ArrayList<String> fields) {
        if (filterIndex != null) {
            /*
             * Check that the value for each field that has a filter is included. Clear fields on the first
             * that is not and exit check loop to read the next line.
             */
            for (int i = 0; i < filterIndex.size(); i++) {
                Integer index = filterIndex.get(i);

                if (index != null && !filter.isIncluded(index, fields.get(i))) return true;
            }
        }
        return false;
    }
    /**
     * This method gets the next input line either from the global variable lastLine, if it is not null, or using
     * getLine. The global lastLine is intended to give sub classes the option to read one line ahead and to insert the
     * line into the input stream by copying it to lastLine. The global is set to null on exit.
     *<p>
     * The input line is converted to an array of strings using the global fieldDelimiter.
     *<p>
     * Any class that overrides this method must implement the above handling of lastLine unless it overrides
     * open(file, setHeader) as well.
     *
     * @return array of fields or null if the input stream is empty.
     *
     * @throws IOException
     */
    protected ArrayList<String> getFields() throws IOException {
        ArrayList<String> fields = null;
        String            line;
        
        while (fields == null) {
            if (tagFields != null && tagIndex < tags.size()) {
                fields = getTagFields();
            } else {
                line     = lastLine == null? getLine() : lastLine;
                lastLine = null;

                if (line == null) break;

                if (tagColumn == null)
                    fields    = unpack(line, columns == null? 0 : columns.size());
                else {
                    tagFields = unpack(line, tagColumns.size());
                    tagIndex  = 0;
                    fields    = getTagFields();
                }
            }
            if (filterExcludes(fields)) fields = null;
        }
        return fields;
    }
    protected void setStream(InputStream stream) {
        this.stream = stream;
        this.reader = new BufferedReader(new InputStreamReader(stream));
    }
    /**
     * Opens stream.This method establishes the columns returned by getColumns.<p>
     * If setColumns is false, it is up to the implementing class to determine how the columns are derived. On completion
     * of open the global columns must be initialised to the column headings for the file data.
     *
     * @param stream to be opened
     * @param reference
     * @reference stream reference used to identify stream in reports
     * @param setColumns if false the implementing class is responsible for deriving the columns.
     *
     * @throws IOException
     */
    protected void open(InputStream stream, String reference, boolean setColumns) throws IOException {
        this.type        = "FileTable";
        this.reference   = reference;
        this.count       = 0;
        this.filterIndex = null;

        setStream(stream);

        if (setColumns) {
            String save = getLine();
                    
            lastLine = save;
            
            if (hasHeader) {
                createColumns(lastLine);
                lastLine = null;
            }
            else {
                createColumns(getFields().size());
                lastLine = save; //Restore lastLine so that it is returned by next call to getFields.
            }
        }
        applyPivot();
        setTagMap();
    }
    /**
     * Opens stream.This method establishes the columns returned by getColumns.
     *
     * @param stream to be opened
     * @param reference
     * @reference stream reference used to identify stream in reports
     *
     * @throws IOException
     */
    protected void open(InputStream stream, String reference) throws IOException {
        open(stream, reference, true);
    }
    /**
     * Opens file and creates the columns array which may require the first line to be read.
     *<p>
     * The columns remain in force until the next open, i.e. they will not change.
     *
     * @param file
     * @throws IOException
     */
    public void open(File file) throws IOException {
        open(new FileInputStream(file), file.getName().split("\\.")[0], true);
    }
    /**
     * Opens stream and creates the columns array.This is not implement and must be called via a sub class that
 overrides it with an implementation.
     * 
     * @param stream
     * @param reference
     * @param properties
     * @throws IOException
     */
    public void open(InputStream stream, String reference, Properties properties) throws IOException {
        type = "FileTable";
        throwIOError("Reader type " + type + " does not support properties");
    }
    /**
     * Opens file and creates the columns array. 
     *
     * @param file
     * @param properties
     * @throws IOException
     */
    public void open(File file, Properties properties) throws IOException {
        open(new FileInputStream(file), file.getName().split("\\.")[0], properties);
    }
    
    @Override
    public String getType() {
        return type;
    }

    /**
     * Returns an identifier that enables the data source to be identified in reports. It defaults to the
     * file name without the path.
     *
     * @return source reference
     */
    @Override
    public String getReference() {
        return reference == null? "" : reference;
     }

    @Override
    public ArrayList<String> getColumns() {
        if (reader != null && pivotFirst != -1) {
            ArrayList<String> cols = new ArrayList<>();

            for (int i = 0; i < pivotFirst; i++) cols.add(this.columns.get(i));

            cols.add(pivotKeyColumn);
            cols.add(pivotValueColumn);
            return cols;
        }
        return reader == null? null : columns;
    }

    /**
     * Gets the next stream input line, ignoring empty lines, and converts it to an array of strings using the
     * defined separator. The array is always the same size as columns. If the line contains less than columns fields
     * the array is padded with null strings so that it is the same size as columns.
     * 
     * Empty fields will be null, i.e. not an empty string.
     *
     * @return field array or null if the input stream is empty
     *
     * @throws IOException IO error or line has more fields than columns.
     */
    @Override
    public ArrayList<String> getValues() throws IOException {
        ArrayList<String> values = null;

        if (pivotIndex == 0) {
            currentValues = getFields();
            
            if (currentValues != null) {
                if (currentValues.size() > columns.size()) throwIOError(currentValues.size() + " values but only " + columns.size() + " columns");

                while (currentValues.size() < columns.size()) {
                    currentValues.add(null);
                }
            }                
        }
        if (currentValues != null) {
            if (pivotFirst != -1) {
                values = new ArrayList<>();

                for (int i = 0; i < pivotFirst; i++) values.add(currentValues.get(i));

                values.add(columns.get(pivotFirst + pivotIndex));
                values.add(currentValues.get(pivotFirst + pivotIndex));
                pivotIndex++;

                if (pivotFirst + pivotIndex >= currentValues.size()) pivotIndex = 0;
            } else
                values = currentValues;
        }
        return values;
    }
    /**
     * Returns a string that identifies the location of the current data line in the source file. This is intended
     * for reports so that users can find the location in the file of the data referenced in a report.
     *
     * @return location string.
     */

    @Override
    public String getLocation() {
        return getReference() + '(' + count + ')';
    }

    /**
     * Each line in the file is assigned a number which starts at 1.
     *
     * @return the file line number of the last line read
     */
    public int getLineNo() {
        return count;
    }

    public void close() throws IOException {
        reader.close();
        stream.close();
        reader = null;
        stream = null;
    }

    @Override
    public void setLogger(Logger log) {
        this.log = log;
    }
    @Override
    public void setFilter(SourceFilter filter) {
        this.filter = filter;
    }
    public void setTaggedColumn(String columnName, char tagSeparator) throws IOException {
        if (reader         != null) throw new IOException("Can't set tagged column for open stream " + getReference());
        if (pivotKeyColumn != null) throw new IOException("Can't set both Pivot and Tagged column "  + getReference());
        
        this.tagColumn    = columnName;
        this.tagSeparator = "" + tagSeparator;
    }
    /**
     * @param lastKeyColumn
     * @param pivotKeyColumn
     * @param pivotValueColumn
     * @throws java.io.IOException
     */
    public void setPivot(String lastKeyColumn, String pivotKeyColumn, String pivotValueColumn) throws IOException {
        if (reader    != null) throw new IOException("Can't set Pivot for open stream "        + getReference());
        if (tagColumn != null) throw new IOException("Can't set both Pivot and Tagged column " + getReference());
        
        this.lastKeyColumn    = lastKeyColumn;
        this.pivotKeyColumn   = pivotKeyColumn;
        this.pivotValueColumn = pivotValueColumn;
    }
    /**
     * @return True if the first line in the file contains column headers.
     */
    public boolean hasHeader() {
        return hasHeader;
    }
    /**
     * Indicates if the first line of the file contains column headers.If set to false, the number of
 fields in the first line defines the number of columns. A column name is created for each field by appending
 the column number to the text Col. Column numbers start at 1 and the number is converted without leading spaces or
 zeros, e.g the name for column 1 is Col1 and the name for column 10 is Col10.
<p>
     * Existing columns are cleared.
     *<p>
     * The columns are extracted when the file is opened.
     *<p>
     * The effect of this method remains in force until it is called again, i.e. it is not set to the default,
     * which is true, on close.
     *
     * A fatal error is reported if this method is called while a file is open.
     *
     * @param hasHeader true if the first line does contain column headers.
     * @throws java.io.IOException
     */
    public void setHasHeader(boolean hasHeader) throws IOException {
        if (reader != null) throw new IOException("Can't set hasHeader for open stream " + getReference());

        this.hasHeader = hasHeader;
    }
    /**
     * Defines the maximum number of fields in any line of file data.This has the same effect as calling
     * setHasHeader(false), except that the number of columns is defined by count and not by the number of fields in
     * the first line of data.<p>
     * The effect of this method remains in force until it, or setHasHeader, is called again, i.e. it is not set to
     * the default, which is true, on close.
     *
     * @param count Maximum number of fields in any line of file data.
     * @throws java.io.IOException
     */
    public void setColumnCount(int count) throws IOException {
        setHasHeader(true);
    }
}
