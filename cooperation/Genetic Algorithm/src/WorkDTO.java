public class WorkDTO {
    
    Double CT;                      // Stitching CycleTime
    Integer MP;                     // Stitching Workers
    Integer preit_used_stitiching;  // Prefit use at Stitching
    Integer PF;                     // Prefit CycleTime

    public Double getCT(){return CT;}

    public void setCT(Double _ct){this.CT = _ct;}

    public Integer getMP(){return MP;}

    public void setMP(Integer _mp){this.MP = _mp;}

    public Integer getPF(){return PF;}

    public void setPF(Integer _pf){this.PF = _pf;}

    public Integer getPUS(){return preit_used_stitiching;}

    public void setPUS(Integer _pus){this.preit_used_stitiching = _pus;}
}
