/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.csc.pe.fileloader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

/**
 *
 * @author CClose
 */
public class SourceFilter {
    private class ColumnFilter {
        protected String name;
        protected ArrayList<String> values     = null;
        private   boolean           ignoreCase = true;

        protected ColumnFilter(String name, ArrayList<String> values, boolean ignoreCase) {
            this.name       = name;
            this.values     = values;
            this.ignoreCase = ignoreCase;

            if (ignoreCase) {
                for (int i = 0; i < values.size(); i++) values.set(i, values.get(i).toLowerCase());
            }
        }

        protected ColumnFilter(String name, ArrayList<String> values) {
            this(name, values, true);
        }

        protected boolean isIncluded(String value) {
            return value != null && values.contains(ignoreCase? value.toLowerCase() : value);
        }
    }
    ArrayList<ColumnFilter> columns = new ArrayList<ColumnFilter>();

    public int getIndex(String name) {
        for (int i = 0; i < columns.size(); i++) {
            if (columns.get(i).name.equalsIgnoreCase(name)) return i;
        }
        return -1;
    }
    public int addColumn(String name, ArrayList<String> allowedValues) {
        columns.add(new ColumnFilter(name, allowedValues));

        return columns.size() - 1;
    }
    public boolean isIncluded(String column, String value) {
        int i = getIndex(column);

        if (i == -1) return true; //If no explicit included values list then all values are allowed;

        return columns.get(i).isIncluded(value);
    }
    public boolean isIncluded(int column, String value) {
        return columns.get(column).isIncluded(value);
    }
    public void setColumns(File file) throws FileNotFoundException, IOException {
        BufferedReader cols  = new BufferedReader(new FileReader(file));
        String         line  = null;
        int            count = 0;

        while ((line = cols.readLine()) != null) {
            count++;

            if (!line.trim().isEmpty()) {
                String name  = line;
                String value = null;
                int    i     = line.indexOf('=');

                if (i != -1) {
                    value = line.substring(i + 1);
                    name  = line.substring(0, i);
                }
                addColumn(name, value == null? null : new ArrayList<String>(Arrays.asList(value.split(","))));
            }
        }
        cols.close();
    }

}
