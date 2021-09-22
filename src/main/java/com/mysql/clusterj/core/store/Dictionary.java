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
     */
    public void createAndRegisterEvent(Event event);

    /**
     * Get an Event already registered with the server.
     * @param eventName The unique identifier of the event.
     * @return The event.
     */
    public Event getEvent(String eventName);

    /**
     * Delete/remove/drop the event identified by the unique event name from the server.
     * @param eventName Unique identifier of the event to be dropped.
     * @param force Not sure what this does.
     */
    public void dropEvent(String eventName, int force);
}
