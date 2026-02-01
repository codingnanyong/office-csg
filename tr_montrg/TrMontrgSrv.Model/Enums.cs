namespace CSG.MI.TrMontrgSrv.Model
{
    public enum MediumType
    {
        unknown = -1,
        cfg,
        ir,
        rgb,
        temp,
        raw
    }

    public enum MediumFileType
    {
        unknown = -1,
        json,
        csv,
        jpg
    }

    public enum Plant
    {
        HQ,
        VJ,
        JJ,
        CKP,
        JJS,
        RJ,
        QD
    }

    public enum DiffLevel
    {
        Unknown = -1,
        A,
        B,
        C,
        D
    }

    public enum EventType
    {
        NoIssue,
        AboveWarning,
        DiffLevel,
        AboveIUcl,
        BelowILcl,
        AboveMrUcl,
        NelsonRule
    }

    public enum EventLevel
    {
        Urgent,
        Warning,
        Info,
        Error
    }

    public enum InspArea
    {
        Frame,
        ROI,
        Box
    }

    public enum NelsonRules
    {
        Rule1 = 1,
        Rule2,
        Rule3,
        Rule4,
        Rule5,
        Rule6,
        Rule7,
        Rule8
    }

    public enum FileType
    {
        bat = 0,
        sh
    }
}
