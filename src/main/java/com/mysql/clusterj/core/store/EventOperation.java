package com.mysql.clusterj.core.store;

import com.mysql.ndbjtie.ndbapi.NdbErrorConst;

/**
 * Wrapper around {@link com.mysql.ndbjtie.ndbapi.NdbEventOperationConst}.
 */
public interface EventOperation {
    public int isOverrun();

    public boolean isConsistent();

    public int /*_NdbDictionary.Event.TableEvent_*/ getEventType();

    public boolean tableNameChanged();

    public boolean tableFrmChanged();

    public boolean tableFragmentationChanged();

    public boolean tableRangeListChanged();

    public long /*_Uint64_*/ getGCI();

    public int /*_Uint32_*/ getAnyValue();

    public long /*_Uint64_*/ getLatestGCI();

    public NdbErrorConst /*_const NdbError &_*/ getNdbError();
}
