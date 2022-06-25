package cn.edu.thssdb.query;

public enum LogicAtom {
    or, and,
    eq, leq, geq, neq, less, great,
    num, str, nul;
    public LogicType getType() {
        if (this == or || this == and) return LogicType.bool;
        if (this == num || this == str || this == nul) return LogicType.atom;
        return LogicType.comp;
    }
}
