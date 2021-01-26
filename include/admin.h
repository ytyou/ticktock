/*
    TickTock is an open-source Time Series Database for your metrics.
    Copyright (C) 2020-2021  Yongtao You (yongtao.you@gmail.com),
    Yi Lin (ylin30@gmail.com), and Yalei Wang (wang_yalei@yahoo.com).

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#pragma once

#include "http.h"
#include "kv.h"
#include "task.h"


namespace tt
{


/* Handles various administrative commands received by the HTTP server.
 */
class Admin
{
public:
    static bool http_post_api_admin_handler(HttpRequest& request, HttpResponse& response);

private:
    Admin() = delete;

    static bool cmd_compact(KeyValuePair *params, HttpResponse& response);
    static bool cmd_log(KeyValuePair *params, HttpResponse& response);
    static bool cmd_ping(KeyValuePair *params, HttpResponse& response);
    static bool cmd_stat(KeyValuePair *params, HttpResponse& response);
    static bool cmd_stop(KeyValuePair *params, HttpResponse& response);

    static bool shutdown(TaskData& data);
};


}
