package com.mysql.clusterj.tie;

import com.mysql.clusterj.ClusterJException;
import com.mysql.clusterj.ColumnType;
import com.mysql.clusterj.core.store.Column;
import com.mysql.clusterj.core.store.RecordAttr;
import com.mysql.clusterj.core.util.I18NHelper;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactoryService;
import com.mysql.ndbjtie.ndbapi.NdbRecAttr;

import java.nio.ByteBuffer;

/**
 * NdbRecAttr
 * Contains value of an attribute.
 *
 * NdbRecAttr objects are used to store the attribute value
 * after retrieving the value from the NDB Cluster using the method
 * NdbOperation::getValue.  The objects are allocated by the NDB API.
 * An example application program follows:
 *
 *   MyRecAttr = MyOperation->getValue("ATTR2", NULL);
 *   if (MyRecAttr == NULL) goto error;
 *
 *   if (MyTransaction->execute(Commit) == -1) goto error;
 *
 *   ndbout << MyRecAttr->u_32_value();
 *
 *       The NdbRecAttr object is instantiated with its value when
 *       NdbTransaction::execute is called.  Before this, the value is
 *       undefined.  (NdbRecAttr::isNULL can be used to check
 *       if the value is defined or not.)
 *       This means that an NdbRecAttr object only has valid information
 *       between the time of calling NdbTransaction::execute and
 *       the time of Ndb::closeTransaction.
 *       The value of the null indicator is -1 until the
 *       NdbTransaction::execute method have been called.
 *
 * For simple types, there are methods which directly getting the value
 * from the NdbRecAttr object.
 *
 * To get a reference to the value, there are two methods:
 * NdbRecAttr::aRef (memory is released by NDB API) and
 * NdbRecAttr::getAttributeObject (memory must be released
 * by application program).
 * The two methods may return different pointers.
 *
 * There are also methods to check attribute type, attribute size and
 * array size.
 * The method NdbRecAttr::arraySize returns the number of elements in the
 * array (where each element is of size given by NdbRecAttr::attrSize).
 * The NdbRecAttr::arraySize method is needed when reading variable-sized
 * attributes.
 *
 * Variable-sized attributes are not yet supported.
 */
public class NdbRecordAttrImpl implements RecordAttr {
    /** My message translator */
    static final I18NHelper local = I18NHelper
            .getInstance(NdbRecordAttrImpl.class);

    /** My logger */
    static final Logger logger = LoggerFactoryService.getFactory()
            .getInstance(NdbRecordAttrImpl.class);

    private final NdbRecAttr ndbRecAttr;
    private final ByteBuffer aValue;
    private final String tableName;

    public NdbRecordAttrImpl(NdbRecAttr ndbRecAttr, String tableName, ByteBuffer aValue) {
        this.ndbRecAttr = ndbRecAttr;
        this.tableName = tableName;
        this.aValue = aValue;
    }

    public Column getColumn() {
        return new ColumnImpl(tableName, ndbRecAttr.getColumn());
    }

    public int getType() {
        return ndbRecAttr.getType();
    }

    public int get_size_in_bytes() {
        return ndbRecAttr.get_size_in_bytes();
    }

    public boolean isNULL() throws ClusterJException {
        int isNull = ndbRecAttr.isNULL();

        System.out.println("isNull: " + isNull);

        if (isNull == 0)
            return true;
        else
            logger.warn("Record attribute is undefined.");

        return false;
    }

    public long int64_value() {
        return ndbRecAttr.int64_value();
    }

    public int int32_value() {
        return ndbRecAttr.int32_value();
    }

    public int medium_value() {
        return ndbRecAttr.medium_value();
    }

    public short short_value() {
        return ndbRecAttr.short_value();
    }

    public byte char_value() {
        return ndbRecAttr.char_value();
    }

    public byte int8_value() {
        return ndbRecAttr.int8_value();
    }

    public long u_64_value() {
        return ndbRecAttr.u_64_value();
    }

    public int u_32_value() {
        return ndbRecAttr.u_32_value();
    }

    public int u_medium_value() {
        return ndbRecAttr.u_medium_value();
    }

    public short u_short_value() {
        return ndbRecAttr.u_short_value();
    }

    public byte u_char_value() {
        return ndbRecAttr.u_char_value();
    }

    public byte u_8_value() {
        return ndbRecAttr.u_8_value();
    }

    public float float_value() {
        return ndbRecAttr.float_value();
    }

    public double double_value() {
        return ndbRecAttr.double_value();
    }

    public RecordAttr cloneNative() {
        NdbRecAttr ndbRecAttrCopy = ndbRecAttr.cloneNative();

        ByteBuffer aValue = this.aValue.duplicate();

        return new NdbRecordAttrImpl(ndbRecAttrCopy, this.tableName, aValue);
    }
}
