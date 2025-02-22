/*
 *  Copyright (c) 2014, Oracle and/or its affiliates. All rights reserved.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation; version 2 of the License.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.clusterj.tie;

import java.nio.ByteBuffer;

import java.util.List;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.clusterj.ClusterJFatalInternalException;
import com.mysql.clusterj.ClusterJUserException;

import com.mysql.clusterj.core.store.ClusterTransaction;

import com.mysql.clusterj.core.store.EventOperation;
import com.mysql.clusterj.core.util.I18NHelper;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactoryService;

import com.mysql.ndbjtie.ndbapi.*;
import com.mysql.ndbjtie.ndbapi.NdbIndexScanOperation.IndexBound;
import com.mysql.ndbjtie.ndbapi.NdbScanOperation.ScanOptions;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.Dictionary;

/**
 * This class is used to hold the ndb Dictionary used for NdbRecord. It has the minimum
 * implementation needed to implement the life cycle of the Ndb. In particular, it omits the
 * buffer manager and partition key scratch buffer used in the standard DbImpl.
 */
class DbImplForNdbRecord implements com.mysql.clusterj.core.store.Db {

    /** My message translator */
    static final I18NHelper local = I18NHelper.getInstance(DbImplForNdbRecord.class);

    /** My logger */
    static final Logger logger = LoggerFactoryService.getFactory()
            .getInstance(DbImplForNdbRecord.class);

    /** The Ndb instance that this instance is wrapping */
    private Ndb ndb;

    /** A "big enough" size for error information */
    private int errorBufferSize = 300;

    /** The ndb error detail buffer */
    private ByteBuffer errorBuffer = ByteBuffer.allocateDirect(errorBufferSize);

    /** The NdbDictionary for this Ndb */
    private Dictionary ndbDictionary;

    /** The ClusterConnection */
    private ClusterConnectionImpl clusterConnection;

    /** This db is closing */
    private boolean closing = false;

    public DbImplForNdbRecord(ClusterConnectionImpl clusterConnection, Ndb ndb) {
        this.clusterConnection = clusterConnection;
        this.ndb = ndb;
        int returnCode = ndb.init(1);
        handleError(returnCode, ndb);
        ndbDictionary = ndb.getDictionary();
        handleError(ndbDictionary, ndb);
    }

    protected void assertOpen(String where) {
        if (closing || ndb == null) {
            throw new ClusterJUserException(local.message("ERR_Db_Is_Closing", where));
        }
    }

    protected void closing() {
        closing = true;
    }

    public void close() {
        Ndb.delete(ndb);
        clusterConnection.close(this);
    }

    public com.mysql.clusterj.core.store.Dictionary getDictionary() {
        throw new ClusterJFatalInternalException(local.message("ERR_Implementation_Should_Not_Occur"));
    }

    public Dictionary getNdbDictionary() {
        return ndbDictionary;
    }

    public ClusterTransaction startTransaction(String joinTransactionId) {
        throw new ClusterJFatalInternalException(local.message("ERR_Implementation_Should_Not_Occur"));
    }

    protected void handleError(int returnCode, Ndb ndb) {
        if (returnCode == 0) {
            return;
        } else {
            NdbErrorConst ndbError = ndb.getNdbError();
            String detail = getNdbErrorDetail(ndbError);
            Utility.throwError(returnCode, ndbError, detail);
        }
    }

    protected void handleError(Object object, Ndb ndb) {
        if (object != null) {
            return;
        } else {
            NdbErrorConst ndbError = ndb.getNdbError();
            String detail = getNdbErrorDetail(ndbError);
            Utility.throwError(null, ndbError, detail);
        }
    }

    public boolean isRetriable(ClusterJDatastoreException ex) {
        throw new ClusterJFatalInternalException(local.message("ERR_Implementation_Should_Not_Occur"));
    }

    public EventOperation createEventOperation(String eventName) {
        throw new UnsupportedOperationException("Not implemented!");
    }

    public boolean dropEventOperation(EventOperation eventOp) {
        throw new UnsupportedOperationException("Not implemented!");
    }

    public boolean pollEvents(int aMillisecondNumber, long[] latestGCI) {
        throw new UnsupportedOperationException("Not implemented!");
    }

    public EventOperation nextEvent() {
        throw new UnsupportedOperationException("Not implemented!");
    }

    public String getNdbErrorDetail(NdbErrorConst ndbError) {
        return ndb.getNdbErrorDetail(ndbError, errorBuffer, errorBuffer.capacity());
    }

    public NdbTransaction enlist(String tableName, List<KeyPart> keyParts) {
        throw new ClusterJFatalInternalException(local.message("ERR_Implementation_Should_Not_Occur"));
    }

    public NdbTransaction enlist(String tableName, int partitionId) {
        throw new ClusterJFatalInternalException(local.message("ERR_Implementation_Should_Not_Occur"));
    }

    public NdbTransaction joinTransaction(String coordinatedTransactionId) {
        throw new ClusterJFatalInternalException(local.message("ERR_Implementation_Should_Not_Occur"));
    }

    /** Initialize ndbjtie infrastructure for query objects created via jtie wrapper create methods.
     * Creating them here avoids race conditions later with multiple threads trying to create
     * them simultaneously. The initialization only needs to be done once.
     */
    protected void initializeQueryObjects() {
        synchronized(ClusterConnectionImpl.class) {
            if (ClusterConnectionImpl.queryObjectsInitialized) {
                return;
            }
            IndexBound indexBound = IndexBound.create();
            if (indexBound != null) IndexBound.delete(indexBound);
            ScanOptions scanOptions = ScanOptions.create();
            if (scanOptions != null) ScanOptions.delete(scanOptions);
            NdbDictionary.Table table = NdbDictionary.Table.create("dummy");
            if (table != null) {
                NdbInterpretedCode ndbInterpretedCode = NdbInterpretedCode.create(table, null, 0);
                if (ndbInterpretedCode != null) {
                    NdbScanFilter ndbScanFilter = NdbScanFilter.create(ndbInterpretedCode);
                    if (ndbScanFilter != null) {
                        NdbScanFilter.delete(ndbScanFilter);
                    }
                    NdbInterpretedCode.delete(ndbInterpretedCode);
                }
                NdbDictionary.Table.delete(table);
            }
            ClusterConnectionImpl.queryObjectsInitialized = true;
        }
    }

}
