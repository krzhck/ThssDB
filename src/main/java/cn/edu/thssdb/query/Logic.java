package cn.edu.thssdb.query;

import cn.edu.thssdb.exception.CompareDifferentTypeException;
import cn.edu.thssdb.schema.Cell;
import cn.edu.thssdb.schema.Column;
import cn.edu.thssdb.type.BoolType;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.type.ComparerType;

public class Logic {
    public Logic left;
    public LogicAtom middle;
    public Logic right;

    public Comparer value;

    public Logic(Comparer a) {
        value = a;
        if (value.mType == ComparerType.STRING) this.middle = LogicAtom.str;
        if (value.mType == ComparerType.NUMBER) this.middle = LogicAtom.num;
        if (value.mType == ComparerType.NULL) this.middle = LogicAtom.nul;
    }

    public Logic(Logic left, LogicAtom middle, Logic right) {
        this.left = left;
        this.middle = middle;
        this.right = right;
    }

    public Logic(Comparer left, LogicAtom middle, Comparer right) {
        this.left = new Logic(left);
        this.middle = middle;
        this.right = new Logic(right);
    }

    Boolean check(int res, LogicAtom comp) {
        if (comp == LogicAtom.eq) return res == 0;
        if (comp == LogicAtom.geq) return res >= 0;
        if (comp == LogicAtom.leq) return res <= 0;
        if (comp == LogicAtom.less) return res < 0;
        if (comp == LogicAtom.neq) return res != 0;
        if (comp == LogicAtom.great) return res > 0;
        return true;
    }

//    boolean compObj(Object a, Object b, LogicAtom type) {
//        if (type == LogicAtom.num)
//            return (double) a < (double) b;
//        if (type == LogicAtom.str)
//            return ((String) a).compareTo((String) b) <= 0;
//        return true;
//    }

    public Object exec(MultiRow row) {
        if (value != null && value.mType == ComparerType.COLUMN) {
            int index = row.getColumnIndex((String) value.mValue);
            Cell cell = row.getEntries().get(index);
            Column column = row.getColumnType(index);
            if (cell.value == null) {
                middle = LogicAtom.nul;
                return null;
            }
            if (column.getColumnType() == ColumnType.STRING) {
                middle = LogicAtom.str;
                return cell.value;
            } else {
                middle = LogicAtom.num;
                return  Double.parseDouble(cell.value.toString());
            }
            // return comparable
//            return entry.value;
        }
        if (value != null && value.mType == ComparerType.NULL) {
            middle = LogicAtom.nul;
            return null;
        }
        if (middle.getType() == LogicType.bool) {
            Object leftResult = left.exec(row);
            Object rightResult = right.exec(row);
            // return Boolean
            if (middle == LogicAtom.and)
            {
                if (leftResult == BoolType.TRUE && rightResult == BoolType.TRUE) return BoolType.TRUE;
                if (leftResult == BoolType.FALSE || rightResult == BoolType.FALSE) return BoolType.FALSE;
                return BoolType.UNKNOWN;
//                return (Boolean) leftResult && (Boolean) rightResult;
            }
            if (middle == LogicAtom.or)
            {
                if (leftResult == BoolType.FALSE && rightResult == BoolType.FALSE) return BoolType.FALSE;
                if (leftResult == BoolType.TRUE || rightResult == BoolType.TRUE) return BoolType.TRUE;
                return BoolType.UNKNOWN;
//                return (Boolean) leftResult || (Boolean) rightResult;
            }
        }
        if (middle.getType() == LogicType.comp) {
            Object leftResult = left.exec(row);
            Object rightResult = right.exec(row);
            if (left.middle == LogicAtom.nul || right.middle == LogicAtom.nul) {
                return BoolType.UNKNOWN;
            }
            if (left.middle != right.middle)
                throw new CompareDifferentTypeException("");
            if (check(((Comparable) leftResult).compareTo(rightResult), middle))
                return BoolType.TRUE;
            else
                return BoolType.FALSE;
        }
        if (middle.getType() == LogicType.atom) {
//            if (middle == LogicAtom.num) return Double.parseDouble(value);
//            return value;
            return value.mValue;
        }
        throw new CompareDifferentTypeException("???");
    }
}
