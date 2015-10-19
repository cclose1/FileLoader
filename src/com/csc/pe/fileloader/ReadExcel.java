/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.csc.pe.fileloader;

/**
 *
 * @author cclose
 */
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Properties;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

/**
 *
 * @author CClose
 */
public class ReadExcel extends FileSource {
    private XSSFWorkbook book   = null;
    private XSSFSheet    sheet  = null;

    private XSSFRow nextRow() {
        XSSFRow row = null;

        while (count < sheet.getPhysicalNumberOfRows() && (row = sheet.getRow(count++)) == null);
        
        return row;
    }
    /**
     * 
     * @return
     * @throws IOException
     */
    protected ArrayList<String> getFields() throws IOException  {
        XSSFRow           row    = nextRow();
        ArrayList<String> fields = null;

        if (row != null) {
            XSSFCell cell = null;

            fields = new ArrayList<String>();

            for (int i = 0; i < row.getPhysicalNumberOfCells(); i++) {
                try {
                    String value = null;

                    cell = row.getCell(i);
                    switch (cell.getCellType()) {
                        case XSSFCell.CELL_TYPE_STRING:
                            value = cell.getRichStringCellValue().getString();
                            break;
                        case XSSFCell.CELL_TYPE_NUMERIC:
                            if (DateUtil.isCellDateFormatted(cell)) {                            
                                value = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss.000").format(cell.getDateCellValue());
                            } else {
                                value = Double.toString(cell.getNumericCellValue());
                            }
                            break;
                        case XSSFCell.CELL_TYPE_BOOLEAN:
                            value = cell.getBooleanCellValue()? "True" : "False";
                            break;
                        case XSSFCell.CELL_TYPE_FORMULA:
                            System.out.println(cell.getCellFormula());
                            throw new Exception("Formulas are not supported");
                    }

                    fields.add(value);
                } catch (Exception ex) {
                    throw new IOException("Sheet " + sheet.getSheetName() + '(' + count + ',' + i + ")-" + ex.getMessage());
                }
            }
        }
        return fields;
    }
    /**
     *
     * @param file
     * @param properties
     * @throws IOException
     */
    public void open(InputStream stream, String reference, Properties properties) throws IOException {
        this.type      = "Excel";
        this.reference = reference;
        this.count     = 0;

        setStream(stream);
        this.book  = new XSSFWorkbook(stream);
        this.sheet = book.getSheetAt(0);        
        
        if (properties != null) {
            Enumeration e = properties.propertyNames();

            while (e.hasMoreElements()) {
                String key = (String) e.nextElement();

                if (key.equalsIgnoreCase("Sheet")) {
                    this.sheet = book.getSheet(properties.getProperty(key));
                } else if (key.equalsIgnoreCase("HeaderLine")) {
                    count = Integer.parseInt(properties.getProperty(key)) - 1;
                } else {
                    throw new IOException("Opening Excel source " + getReference() + " property " + key + " not supported");
                }
            }
        }
        columns = getFields();
    }
    /**
     *
     * @throws IOException
     */
    public void close() throws IOException {
        /*
         * Nothing to do on close. Need to override this method to prevent the attempt to close the reader file.
         */
        sheet = null;
        book  = null;
        super.close();
    }
}
