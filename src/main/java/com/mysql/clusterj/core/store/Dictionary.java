/*
   Copyright (c) 2010, 2011, Oracle and/or its affiliates. All rights reserved.

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

/**
 *
 */
public interface Dictionary {

    public Index getIndex(String indexName, String tableName, String indexAlias);

    public Table getTable(String tableName);

    public void removeCachedTable(String tableName);

    /**
     * Create and register an NDB event with the server based on the ClusterJ Event object.
     * @param event The "template" Event object.
     * @param force This is passed to the dropTable() function if the event we're trying to create already exists,
     *              and we must drop the existing event first.
     */
    public void createAndRegisterEvent(Event event, int force);

    /**
     * Get an Event already registered with the server.
     * @param eventName The unique identifier of the event.
     * @return The event.
     */
    public Event getEvent(String eventName);

    /**
     * Delete/remove/drop the event identified by the unique event name from the server.
     * @param eventName Unique identifier of the event to be dropped.
     * @param force If 1, then do not check if event exists before trying to delete it.
     * @return True if the event was dropped, false if the event did not exist in the first place. 
     */
    public boolean dropEvent(String eventName, int force);
}
