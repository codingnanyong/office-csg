import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.EncryptedDocumentException;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

//Excel 관리 Class
public class ExcelManager {

    //All Values in Object 
    public static List<WorkDTO> getWorkList() throws EncryptedDocumentException, IOException {

        List<WorkDTO> workList = new ArrayList<WorkDTO>();
        
        try {
            File path = new File("files/inputData.xlsx");
            FileInputStream inputStream = new FileInputStream(path);

            Workbook workbook = WorkbookFactory.create(inputStream);

            Sheet sheet = workbook.getSheetAt(0);
            Iterator<Row> rowltr = sheet.iterator();

            while (rowltr.hasNext()) {
                WorkDTO work = new WorkDTO();
                Row row = rowltr.next();

                if (row.getRowNum() == 0) {
                    continue;
                }
                Iterator<Cell> cellltr = row.cellIterator();
                while (cellltr.hasNext()) {
                    Cell cell = cellltr.next();
                    int index = cell.getColumnIndex();
                    switch (index) {
                        case 2:
                            work.setCT(Double.parseDouble(String.format("%.2f", ((Double) getValueFromCell(cell)))));
                            break;
                        case 3:
                            work.setMP(((Double) getValueFromCell(cell)).intValue());
                            break;
                        case 6:
                            work.setPUS(((Double) getValueFromCell(cell)).intValue());
                            break;
                        case 7:
                            work.setPF(((Double) getValueFromCell(cell)).intValue());
                            break;
                    }
                }
                workList.add(work);
            }
            return workList;

        } catch (Exception e) {
            e.printStackTrace();
        }
        return workList;
    }

    //Cell의 DataType별 return value
    public static Object getValueFromCell(Cell cell) {

        switch (cell.getCellType()) {
            case NUMERIC:
                if (DateUtil.isCellDateFormatted(cell)) {
                    return cell.getDateCellValue();
                }
                return cell.getNumericCellValue();
            case STRING:
                return cell.getStringCellValue();
            case BLANK:
                return "";
            case BOOLEAN:
                return cell.getBooleanCellValue();
            case ERROR:
                break;
            case FORMULA:
                break;
            case _NONE:
                break;
            default:
                return "";
        }
        return cell;
    }
}
