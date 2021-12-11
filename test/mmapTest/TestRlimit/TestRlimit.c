
// C program to demonstrate working of getrlimit() 
// and setlimit()
//
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/resource.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
  
int main() {
  
    struct rlimit old_lim, lim, new_lim;
    
    if( getrlimit(RLIMIT_DATA, &old_lim) == 0)
        printf("Old limits -> soft limit= %" PRIu64 "\t"
           " hard limit= %" PRIu64 "\n", old_lim.rlim_cur, 
                                 old_lim.rlim_max);
    else
        fprintf(stderr, "%s\n", strerror(errno));

  
    //lim.rlim_cur = 200000000;
    lim.rlim_cur = 2<<10;
    //lim.rlim_max = 300000000;
    lim.rlim_max = RLIM_INFINITY; //INT32_MAX;

    printf("RLIM_INFINITY:%" PRIu64 "\n", RLIM_INFINITY);
    printf("RLIM_INFINITY:%" PRIu64 "\n", lim.rlim_max);

    // Get old limits
    if( setrlimit(RLIMIT_DATA, &lim) == -1)
        fprintf(stderr, "%s\n", strerror(errno));


    /* Retrieve and display new CPU time limit */
    if (getrlimit(RLIMIT_DATA, &new_lim) == -1)
        fprintf(stderr, "%s\n", strerror(errno));
    else
        printf("New limits: soft=%ld; hard=%" PRIu64 "\n",
                   new_lim.rlim_cur, new_lim.rlim_max);

    return 0;
}
