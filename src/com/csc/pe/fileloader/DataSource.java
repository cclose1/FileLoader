/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package com.csc.pe.fileloader;

import org.cbc.utils.system.Logger;
import java.io.IOException;
import java.util.ArrayList;

/**
 * @author CClose
 *
 *
 */
public interface DataSource {
    /*
     * Returns the data type.
     */
    public String getType();

    /*
     * Unique reference for the data source, e.g. for a file source this could be the file.
     *
     */
    public String getReference();

    public ArrayList<String> getColumns();

    public ArrayList<String> getValues() throws IOException;

    public void setLogger(Logger log);

    public String getLocation();

    public void setFilter(SourceFilter filter);
}
