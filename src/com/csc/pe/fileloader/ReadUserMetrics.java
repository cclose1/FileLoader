/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.csc.pe.fileloader;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

/**
 *
 * @author CClose
 */
public class ReadUserMetrics extends FileSource {
    private String environment;
    private int minCols = 1;

    public ArrayList<String> getValues() throws IOException {
        ArrayList<String> values = null;
        String line = null;

        while ((line = getLine()) != null) {
            if (line.length() != 0) {
                String fields[] = line.split(",");
                
                if (fields.length < minCols) throw new IOException("Stream " + reference + " requires at least " + minCols + " fields");
                
                values = new ArrayList<String>();
                values.add(environment);
                values.add(fields[0]);

                if (fields.length > minCols) {
                    if (type.equalsIgnoreCase("UniqueUsers")) values.add(fields[1]);
                    
                    String flds[] = fields[fields.length - 1].split("\\."); //Remove the .0 from the integer value

                    values.add(flds[0]);
                }
                break;
            }
        }
        return values;
    }
    public void open(InputStream stream, String reference) throws IOException {
        String line = null;

        columns = new ArrayList<String>();
        open(stream, reference, false);
        columns.add("Environment");
        columns.add("Timestamp");
        
        type           = "BusyUsers";
        environment    = "PRODLOROLTP";
        minCols        = 1;
        
        if (reference.toLowerCase().indexOf("uniqueusers") != -1) {
            columns.add("Trust");
            type    = "UniqueUsers";
            minCols = 2;
        }
        else if (reference.toLowerCase().indexOf("busyusers") == -1)throw new IOException("Stream " + reference + " type " + type + " is invalid");
        
        columns.add("Users");

        while ((line = getLine()) != null) {
            count += 1;
            String[] flds = line.split(",");

            if (line == null) {
                reader.close();
                throw new IOException("Stream " + reference + " header not found");
            }
            if (flds[0].equalsIgnoreCase("# time")) {
                if (!type.equalsIgnoreCase("UniqueUsers")) {
                    reader.close();
                    throw new IOException("Stream " + reference + " header invalid");
                }
                break;
            } else if (flds[0].equalsIgnoreCase("timestamp")){
                if (!type.equalsIgnoreCase("BusyUsers")) {
                    reader.close();
                    throw new IOException("Stream " + reference + " header invalid");
                }
                break;
            }
        }
    }
}
