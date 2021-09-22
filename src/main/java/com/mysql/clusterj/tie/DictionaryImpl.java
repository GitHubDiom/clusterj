/*
 *  Copyright (c) 2010, 2012, Oracle and/or its affiliates. All rights reserved.
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

import com.mysql.ndbjtie.ndbapi.NdbDictionary.Dictionary;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.Event;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.DictionaryConst;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.EventConst;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.DictionaryConst.ListConst.Element;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.DictionaryConst.ListConst.ElementArray;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.IndexConst;
import com.mysql.ndbjtie.ndbapi.NdbDictionary.TableConst;

import com.mysql.clusterj.core.store.Index;
import com.mysql.clusterj.core.store.Table;

import com.mysql.clusterj.core.util.I18NHelper;
import com.mysql.clusterj.core.util.Logger;
import com.mysql.clusterj.core.util.LoggerFactoryService;
import com.mysql.ndbjtie.ndbapi.NdbErrorConst;

/**
 *
 */
class DictionaryImpl implements com.mysql.clusterj.core.store.Dictionary {

    /** My message translator */
    static final I18NHelper local = I18NHelper
            .getInstance(DictionaryImpl.class);

    /** My logger */
    static final Logger logger = LoggerFactoryService.getFactory()
            .getInstance(DictionaryImpl.class);

    private Dictionary ndbDictionary;

    private ClusterConnectionImpl clusterConnection;

    public DictionaryImpl(Dictionary ndbDictionary, ClusterConnectionImpl clusterConnection) {
        this.ndbDictionary = ndbDictionary;
        this.clusterConnection = clusterConnection;
    }

    public Table getTable(String tableName) {
        TableConst ndbTable = ndbDictionary.getTable(tableName);
        if (ndbTable == null) {
            // try the lower case table name
            ndbTable = ndbDictionary.getTable(tableName.toLowerCase());
        }
        if (ndbTable == null) {
            return null;
        }
        return new TableImpl(ndbTable, getIndexNames(ndbTable.getName()));
    }

    public Index getIndex(String indexName, String tableName, String indexAlias) {
        if ("PRIMARY$KEY".equals(indexName)) {
            // create a pseudo index for the primary key hash
            TableConst ndbTable = ndbDictionary.getTable(tableName);
            if (ndbTable == null) {
                // try the lower case table name
                ndbTable = ndbDictionary.getTable(tableName.toLowerCase());
            }
            handleError(ndbTable, ndbDictionary, "");
            return new IndexImpl(ndbTable);
        }
        IndexConst ndbIndex = ndbDictionary.getIndex(indexName, tableName);
        if (ndbIndex == null) {
            // try the lower case table name
            ndbIndex = ndbDictionary.getIndex(indexName, tableName.toLowerCase());
        }
        handleError(ndbIndex, ndbDictionary, indexAlias);
        return new IndexImpl(ndbIndex, indexAlias);
    }

    public String[] getIndexNames(String tableName) {
        // get all indexes for this table including ordered PRIMARY
        com.mysql.ndbjtie.ndbapi.NdbDictionary.DictionaryConst.List indexList = 
            com.mysql.ndbjtie.ndbapi.NdbDictionary.DictionaryConst.List.create();
        final String[] result;
        try {
            int returnCode = ndbDictionary.listIndexes(indexList, tableName);
            handleError(returnCode, ndbDictionary, tableName);
            int count = indexList.count();
            result = new String[count];
            if (logger.isDetailEnabled()) logger.detail("Found " + count + " indexes for " + tableName);
            ElementArray elementArray = indexList.elements();
            for (int i = 0; i < count; ++i) {
                Element element = elementArray.at(i);
                handleError(element, ndbDictionary, String.valueOf(i));
                String indexName = element.name();
                result[i] = indexName;
            }
        } finally {
            // free the list memory even if error
            com.mysql.ndbjtie.ndbapi.NdbDictionary.DictionaryConst.List.delete(indexList);
        }
        return result;
    }

    protected static void handleError(int returnCode, DictionaryConst ndbDictionary, String extra) {
        if (returnCode == 0) {
            return;
        } else {
            Utility.throwError(returnCode, ndbDictionary.getNdbError(), extra);
        }
    }

    protected static void handleError(Object object, DictionaryConst ndbDictionary, String extra) {
        if (object != null) {
            return;
        } else {
            Utility.throwError(null, ndbDictionary.getNdbError(), extra);
        }
    }

    /** Remove cached table from this ndb dictionary. This allows schema change to work.
     * @param tableName the name of the table
     */
    public void removeCachedTable(String tableName) {
        // remove the cached table from this dictionary
        ndbDictionary.removeCachedTable(tableName);
        // also remove the cached NdbRecord associated with this table
        clusterConnection.unloadSchema(tableName);
    }

    /**
     * Create and register an NDB event with the server.
     *
     * @param event ClusterJ representation of the event.
     */
    public void createAndRegisterEvent(com.mysql.clusterj.core.store.Event event) {
        Event ndbEvent = new Event();
        TableConst ndbTable = ndbDictionary.getTable(event.getTableName());
        ndbEvent.setName(event.getName());
        ndbEvent.setDurability(com.mysql.clusterj.EventDurability.convert(event.getDurability()));
        ndbEvent.setReport(com.mysql.clusterj.EventReport.convert(event.getReport()));
        ndbEvent.setTable(ndbTable);

        // Try to register the event.
        int returnCode = ndbDictionary.createEvent(ndbEvent);

        // If an error has occurred, then we'll try to handle it.
        // First, we'll check if the error occurred simply because the event already exists.
        // If that's the case, then we will drop the event and then re-add it.
        // If we still get an error after that, then we'll raise an exception.
        if (returnCode != 0) {
            NdbErrorConst ndbError = ndbDictionary.getNdbError();
            int errorCode = ndbError.code();

            if (errorCode == NdbErrorConst.Classification.SchemaObjectExists) {
                logger.debug("Event creation failed: event " + event.getName() + " already exists.");
                logger.debug("Dropping event " + event.getName());
                dropEvent(event.getName(), 0);

                // Try to add it again. Throw an exception if we get another error.
                returnCode = ndbDictionary.createEvent(ndbEvent);
                if (returnCode > 0) handleError(returnCode, ndbDictionary, "");
            } else {
                // There was some other error (i.e., it wasn't that the event already exists).
                handleError(returnCode, ndbDictionary, "");
            }
        }
    }

    /**
     * Return the event identified by the given name, if it exists.
     * @param eventName The unique identifier of the event.
     * @return The event.
     */
    public com.mysql.clusterj.core.store.Event getEvent(String eventName) {
        EventConst eventConst = ndbDictionary.getEvent(eventName);
        TableConst ndbTable = eventConst.getTable();

        return new EventImpl(
                eventConst.getName(),
                eventConst.getDurability(),
                eventConst.getReport(),
                new TableImpl(ndbTable, getIndexNames(ndbTable.getName())));
    }

    /**
     * Delete/remove/drop the event identified by the unique event name from the server.
     * @param eventName Unique identifier of the event to be dropped.
     * @param force Not sure what this does.
     */
    public void dropEvent(String eventName, int force) {
        int returnCode = ndbDictionary.dropEvent(eventName, force);
        handleError(returnCode, ndbDictionary, "");
    }

    public Dictionary getNdbDictionary() {
        return ndbDictionary;
    }

}
