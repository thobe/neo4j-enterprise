/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
define(function(){return function(vars){ with(vars||{}) { return "<div class=\"sidebar\"><h1 class=\"pad\">Neo4j web administration</h1><ul class=\"info_list\"><li><h3>Server url</h3><p>" + 
server.url + 
"</p></li><li><h3>Kernel version</h3><p>" + 
server.version + 
"</p></li></ul><div class=\"foldout\"><h2><a href=\"#\" class=\"foldout_trigger\">More about Neo4j</a></h2><div class=\"foldout_content pad\"><h2>The Neo4j project</h2><p>For more information, help and examples, please visit <a href=\"http://neo4j.org\">the Neo4j community site</a>.</p></div></div><div class=\"foldout\"><h2><a href=\"#\" class=\"foldout_trigger\">More about Charts</a></h2><div class=\"foldout_content pad\"><h2>Charts</h2><p>To the right are charts that show the total number of primitive entities in the database over time, as well as the heap memory usage in percent.</p><p>You can select the granularity of the charts with the links in the top right corner of each chart.</p><p>To get specific info of some point in any chart, simply hover the mouse over the chart line.</p><p>The charts are automatically updated every three seconds.</p></div></div><div class=\"foldout\"><h2><a href=\"#\" class=\"foldout_trigger\">More about KPIs</a></h2><div class=\"foldout_content pad\"><h2>Key points of interest</h2><p>Below the charts are three boxes showing different sets of KPIs. These are updated every three seconds.</p><h2>A note on disk size</h2><p>Because of the way neo4j stores data, the different storage files (nodestore, relationshipstore etc.) will sometimes be very small before the server has recieved any requests.</p><p>It may be surprising to see the storage files grow rapidly when server first recieves requests, but the measurements will actually be correctly reflecting the disk size of the storage files.</p><p>This is only occurs if you are working on a relatively small graph.</p><h2>A note on primitive counts</h2><p>he number of nodes, properties and relations are estimates based on file sizes. Actually counting the nodes is not done for performance reasons. These are usually very accurate, but in certain cases if you have experienced a database crash and subsequent recovery, they may be very wrong.</p><p>How wrong the numbers are is entirely based on the operations you were doing right before the crash. There is currently no automated way to remedy this problem.</p></div></div></div><div class=\"workarea with-sidebar\"><div id=\"dashboard-info\"></div><div class=\"break\"></div><div id=\"dashboard-charts\"></div></div>";}}; });