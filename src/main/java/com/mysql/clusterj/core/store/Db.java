/*
   Copyright (c) 2010, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

package com.mysql.clusterj.core.store;

import com.mysql.clusterj.ClusterJDatastoreException;
import com.mysql.ndbjtie.ndbapi.NdbEventOperation;

/**
 *
 */
public interface Db {

    public void close();

    /**
     * Get an object for retrieving or manipulating database schema information.
     *
     * This object operates outside any transaction.
     *
     * @return Object containing meta information about all tables
     *         in NDB Cluster.
     */
    public Dictionary getDictionary();

    public ClusterTransaction startTransaction(String joinTransactionId);

    public boolean isRetriable(ClusterJDatastoreException ex);

    /**
     * Create a subcription to an event defined in the database.
     *
     * @param eventName
     *        unique identifier of the event
     *
     * @return Object representing an event, NULL on failure
     */
    public EventOperation createEventOperation(String eventName);

    /**
     * Drop a subscription to an event.
     *
     * @param eventOp
     *        Event operation
     *
     * @return 0 on success
     */
    public boolean dropEventOperation(EventOperation eventOp);

    /**
     * Wait for an event to occur. Will return as soon as an event
     * is available on any of the created events.
     *
     * @param aMillisecondNumber
     *        maximum time to wait
     * aMillisecondNumber < 0 will cause a long wait
     * @param latestGCI
     *        if a valid pointer is passed to a 64-bit integer it will be set
     *        to the latest polled GCI. If a cluster failure is detected it
     *        will be set to NDB_FAILURE_GCI.
     *
     * @return True if events available, false if no events available.
     */
    public boolean pollEvents(int aMillisecondNumber, long[] latestGCI);

    /**
     * This is a backward compatibility wrapper to nextEvent2().
     * Returns an event operation that has data after a pollEvents,
     * NULL if the queue is empty.
     * It maintains the old behaviour :
     * - returns NULL for inconsistent epochs. Therefore, it is important
     *   to call isConsistent(Uint64& gci) to check for inconsistency,
     *   after nextEvent() returns NULL.
     * - will not have empty epochs in the event queue (i.e. remove them),
     * - exits the process when it encounters an event data
     *   representing an event buffer overflow.
     */
    public EventOperation nextEvent();
}
