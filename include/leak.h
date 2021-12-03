/*
    TickTock is an open-source Time Series Database, maintained by
    Yongtao You (yongtao.you@gmail.com) and Yi Lin (ylin30@gmail.com).

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
#include <cstdlib>
#include <string.h>


namespace tt
{


#ifdef _LEAK_DETECTION

#define FREE(X)     ld_free((X), __FILE__, __LINE__)
#define MALLOC(X)   ld_malloc((X), __FILE__, __LINE__)
#define STRDUP(X)   ld_strdup((X), __FILE__, __LINE__)

#define LD_ADD(X)   ld_add((X), __FILE__, __LINE__)
#define LD_DEL(X)   ld_del((X), __FILE__, __LINE__)
#define LD_STATS(X) ld_stats(X)

struct mem_info
{
    size_t size;
    unsigned int line;
    char file[32];
    char thread[32];
};

extern void ld_add(void *p, size_t size, const char *file, int line);
extern void ld_del(void *p, const char *file, int line);
extern unsigned long ld_stats(const char *msg);

extern void  ld_free(void *p, const char *file, int line);
extern void *ld_malloc(std::size_t size, const char *file, int line);
extern char *ld_strdup(const char *str, const char *file, int line);

#else   // _LEAK_DETECTION

#define FREE(X)     free(X)
#define MALLOC(X)   malloc(X)
#define STRDUP(X)   strdup(X)

#define LD_ADD(X)
#define LD_DEL(X)
#define LD_STATS(X)

#endif  // _LEAK_DETECTION


}
