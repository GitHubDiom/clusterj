package com.mysql.clusterj.core.store;

import com.mysql.ndbjtie.ndbapi.NdbDictionary;
import com.mysql.ndbjtie.ndbapi.NdbRecAttr;

public interface RecordAttr {
    /**
     * Get meta information
     */
    Column getColumn();

    /**
     * Get type of column
     * @return Data type of the column
     */
    int getType();

    /**
     * Get attribute (element) size in bytes.
     */
    int get_size_in_bytes();

    /**
     * Check if attribute value is NULL.
     *
     * The NDB variant returns:
     *          -1 = Not defined (Failure or NdbTransaction::execute not yet called).
     *          0 = Attribute value is defined, but not equal to NULL.
     *          1 = Attribute value is defined and equal to NULL.
     *
     * @return false if the value is defined and not equal to null, true if the value is defined and equal to null.
     */
    boolean isNULL();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  64 bit long value.
     */
    long int64_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  32 bit int value.
     */
    int int32_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Medium value.
     */
    int medium_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Short value.
     */
    short short_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Char value.
     */
    byte char_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Int8 value.
     */
    byte int8_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  64 bit unsigned value.
     */
    long u_64_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  32 bit unsigned value.
     */
    int u_32_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Unsigned medium value.
     */
    int u_medium_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Unsigned short value.
     */
    short u_short_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Unsigned char value.
     */
    byte u_char_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Uint8 value.
     */
    byte u_8_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Float value.
     */
    float float_value();

    /**
     * Get value stored in NdbRecAttr object.
     *
     * @return  Double value.
     */
    double double_value();

    /**
     * Make a copy of RecAttr object including all data.
     *
     * The copy needs to be deleted by application program.
     */
    RecordAttr cloneNative();
}
