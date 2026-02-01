import java.io.IOException;
import java.util.List;

import org.apache.poi.EncryptedDocumentException;

public class WorkCondition {

    int prefit = 0;
    int stitching = 0;
    int workers = 0;
    int puas = 0;

    double[][] Stitching_ct;
    int[][] prefit_ct;
    int[][] Stitching_Workers;
    int[][] pus;

    List<WorkDTO> WorkList;

    WorkCondition() {

        try {
            WorkList = ExcelManager.getWorkList();
            this.setCondtion();
            this.setStitching();
            this.setPrefit();
            this.setWorkers();
            this.setPUS();
        } catch (EncryptedDocumentException | IOException e) {
            e.printStackTrace();
        }
        ;

    }

    public void setCondtion() {

        stitching = getMaxIndex("CT");
        prefit = getMaxIndex("PF");
        workers = getMaxIndex("MP");
        puas = getMaxIndex("PUS");

    }

    public int getPrefitCount() {return this.prefit;}

    public int getStitchingCount() {return this.stitching;}

    public int getWorkersCount() { return this.workers;}

    public int getPuasCount() {return this.puas;}

    public double[][] getStitching() {return Stitching_ct;}

    public void setStitching() {

        Stitching_ct = new double[stitching][1];

        for (int i = 0; i < stitching; i++) {
            Stitching_ct[i][0] = WorkList.get(i).getCT();
        }
    }

    public int[][] getPrefit() {return prefit_ct;}

    public void setPrefit() {

        prefit_ct = new int[prefit][1];

        for (int i = 0; i < prefit; i++) {
            prefit_ct[i][0] = WorkList.get(i).getPF();
        }
    }

    public int[][] getWorkers() {return Stitching_Workers;}

    public void setWorkers() {

        Stitching_Workers = new int[workers][1];

        for (int i = 0; i < workers; i++) {
            Stitching_Workers[i][0] = WorkList.get(i).getMP();
        }
    }

    public int[][] getPUS() {return pus;}

    public void setPUS() {

        pus = new int[puas][1];
        
        for (int i = 0; i < puas; i++) {
            pus[i][0] = WorkList.get(i).getPUS();
        }
    }

    public int getMaxIndex(String value) {

        int index = 0;

        for (int i = 0; i < WorkList.size(); i++) {
            switch (value) {
                case "CT":
                    if (WorkList.get(i).getCT() != null) {
                        index++;
                    } else {
                        break;
                    }
                    break;
                case "MP":
                    if (WorkList.get(i).getMP() != null) {
                        index++;
                    } else {
                        break;
                    }
                    break;
                case "PF":
                    if (WorkList.get(i).getPF() != null) {
                        index++;
                    } else {
                        break;
                    }
                    break;
                case "PUS":
                    if (WorkList.get(i).getPUS() != null) {
                        index++;
                    } else {
                        break;
                    }
                    break;
            }
        }
        return index;
    }

}
