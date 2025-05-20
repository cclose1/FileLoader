/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.csc.pe.fileloader;

import org.cbc.filehandler.FileReader;
import org.cbc.filehandler.FileTransfer;
import org.cbc.utils.data.DatabaseSession;
import org.cbc.utils.system.CommandLineReader;
import org.cbc.utils.system.CommandLineReader.CommandLineException;
import org.cbc.utils.system.DateFormatter;
import org.cbc.utils.system.Logger;
import org.cbc.utils.system.Timer;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;
/**
 *
 * @author CClose
 */
public class LoadFiles {

    /**
     * @param args the command line arguments
     * @throws java.lang.InstantiationException
     */
    
    public static void main(String[] args) throws InstantiationException {
        CommandLineReader cmd       = new CommandLineReader();
        DateFormatter     fmtDate   = new DateFormatter("dd-MMM-yy HH:mm:ss");
        Logger            log       = new Logger();
        Timer             timer     = new Timer();
        FileSource        reader    = new ReadUserMetrics();
        LoadData          loader    = new LoadData();
        DatabaseTable     table     = new DatabaseTable();
        String            version   = "V1.3 Released 29-Jan-2013";
        FileReader        fReader   = new FileReader();
        boolean           reportMap = true;
        DatabaseSession   session;
        
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        cmd.addParameter("Server");
        cmd.addParameter("Database");
        cmd.addParameter("Table");
        cmd.addParameter("Source");
        cmd.addOption("ErrorDuplicates");
        cmd.addQualifiedOption("JDBCDriver", "Driver class", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        cmd.addQualifiedOption("JDBCProtocol", "Protocol", "sqlserver");
        cmd.addQualifiedOption("Separator", "Separator character", ",");
        cmd.addQualifiedOption("Headers", "Data Headers", "Y");
        cmd.addQualifiedOption("Recurse", "Flag", "N");
        cmd.addQualifiedOption("Reader", "Reader class");
        cmd.addQualifiedOption("User", "Name");
        cmd.addQualifiedOption("ColumnMap", "File");
        cmd.addQualifiedOption("InputFilter", "File");
        cmd.addQualifiedOption("Password", "Password");
        cmd.addQualifiedOption("Schema", "Schema", "dbo");
        cmd.addQualifiedOption("Move", "Folder");
        cmd.addQualifiedOption("Match", "Regex");
        cmd.addQualifiedOption("Log", "Path");
        cmd.addQualifiedOption("Days", "Oldest file in days");
        cmd.addQualifiedOption("OpenOptions", "Name=Value");
        cmd.setFields("OpenOptions", ',', 1, -1);
        cmd.addQualifiedOption("Pivot", "LastKeyColumn,PivotKeyColumn,PivotValueColumn");
        cmd.setFields("Pivot", ',', 3, 3);
        cmd.addQualifiedOption("Tag", "TagColumn,TagSeparator");
        cmd.setFields("Tag", ',', 2, 2);

        try {
            cmd.load("LoadFiles", version, args, false);
            log.setTimePrefix("HH:mm:ss.SSS");
            log.setLogException(true);

            if (cmd.isPresent("Log")) {
                String file = cmd.getString("Log");

                if (file.indexOf('\'') != -1) {
                    file = fmtDate.format(new Date(), file);
                }
                log.setReportStream(file, true);

                int extIndex = file.lastIndexOf(".log");

                if (extIndex != 0) {
                    file = file.substring(0, extIndex) + ".err";
                } else {
                    file = file + ".err";
                }
                log.setErrorStream(file, true);
                loader.setLog(log);
                table.setLog(log);
            }
            log.comment("Started load from " + cmd.getString("Source") + " to "  + cmd.getString("Server") + '.' + cmd.getString("Database") + '.' + cmd.getString("Table"));

            if (cmd.isPresent("Reader")) {
                try {
                    reader = (FileSource) LoadFiles.class.getClassLoader().loadClass(cmd.getString("Reader")).newInstance();
                } catch (Exception ex) {
                    log.fatalError("Unable to create reader class " + cmd.getString("Reader") + '-' + ex.getMessage());
                }
            } else
                reader = new FileSource();

            fReader.setFilter(cmd.getString("Match", 0, true));
            fReader.setExpandDirectories(cmd.getString("Recurse").equalsIgnoreCase("Y"));

            if (cmd.isPresent("Days")) fReader.setSince(cmd.getInt("Days"));

            ArrayList<FileReader.File> files = fReader.getFiles(cmd.getString("Source"));

            if (files == null || files.isEmpty())
                log.warning("No files" + (cmd.isPresent("Match")? " matching on " + cmd.getString("Match") : "") + " found in " + cmd.getString("Source"));
            else {
                log.comment("Found " + files.size() + " file(s)" + (cmd.isPresent("Match")? " matching on " + cmd.getString("Match") : "") + " in " + cmd.getString("Source"));
                session = new DatabaseSession (
                        cmd.getString("JDBCProtocol"),
                        cmd.getString("Server"),
                        cmd.getString("Database"));
                session.setUser(
                        cmd.getString("User"),
                        cmd.getString("Password"));
                session.connect();
                loader.setLoadLog("LoadLog");
                table.setSession(session);

                if (cmd.isPresent("Separator")) {
                    reader.setFieldDelimiter(cmd.getChar("Separator"));
                }
                reader.setHasHeader(cmd.getString("Headers").equalsIgnoreCase("y"));
                table.setSession(session);
                table.setTable(cmd.getString("Schema"), cmd.getString("Table"));

                if (cmd.isPresent("ColumnMap")) {
                    table.setColumns(new File(cmd.getString("ColumnMap")));
                }
                if (cmd.isPresent("InputFilter")) {
                    SourceFilter filter = new SourceFilter();

                    filter.setColumns(new File(cmd.getString("InputFilter")));
                    reader.setFilter(filter);
                }
                if (cmd.isPresent("Pivot"))
                    reader.setPivot(
                            cmd.getString("Pivot", 0),
                            cmd.getString("Pivot", 1),
                            cmd.getString("Pivot", 2));
                if (cmd.isPresent("Tag"))
                    reader.setTaggedColumn(
                            cmd.getString("Tag", 0),
                            cmd.getChar("Tag", 1));
                loader.setLoadLog(table.getQualifiedTable("LoadLog"));
                
                for (FileReader.File f : files) {
                    try {
                        if (loader.isLoaded(table, reader.getType(), f.getName(), reportMap)) {
                            /*
                             * No action required as already reported
                             */
                        } else {
                            if (cmd.isPresent("OpenOptions")) {
                                reader.open(f.open(), f.getName(), cmd.getProperties("OpenOptions"));
                            } else {
                                reader.open(f.open(), f.getName());
                            }
                            loader.load(reader, table, !cmd.isPresent("ErrorDuplicates"), reportMap);
                            reportMap = false;
                            reader.close();
                        }
                        if (cmd.isPresent("Move")) {
                            try {
                                FileTransfer.moveFile(f.getFile(), new File(cmd.getString("Move")));
                            } catch (IOException ex) {
                                log.warning("Unable to move " + f.getFile().getName() + " to " + cmd.getString("Move"));
                            }
                        }
                    } catch (IOException ex) {
                        log.error("Loading table " + table + " from file " + f.getName() + "-error " + ex.getMessage());
                   }
                }                
                log.comment(timer.addElapsed("Load from " + cmd.getString("Source") + " complete"));
            }
        } catch (IOException ex) {
            log.error("IO error-" + ex.getMessage());
        } catch (SQLException ex) {
            log.error("Database error-" + ex.getMessage());
        } catch (CommandLineException ex) {
            log.error("Command line error-" + ex.getMessage());
        }
        if (log.errorsReported()   && log.isErrorToFile()) System.out.println("Errors reported");
        if (log.warningsReported() && log.isOutToFile())   System.out.println("Warnings reported");
    }
}
