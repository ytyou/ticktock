#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

int main(void) {
  size_t pagesize = getpagesize();

  printf("System page size: %zu bytes\n", pagesize);
  //char * region = mmap(
  char * region = mmap64(
    0,   // Map from the start of the 0th page
    pagesize * 60000,                 // for one page length
    PROT_READ|PROT_WRITE|PROT_EXEC,
    MAP_ANON|MAP_PRIVATE,             // to a private block of hardware memory
    0,
    0
  );

  if (region == MAP_FAILED) {
    perror("Could not mmap");
    return 1;
  }

  int count = 1 << 20;
  
  char tmpStr[4096];
  for(int i=0; i< 4096; i++) {
	tmpStr[i]='1';
  }
  tmpStr[4095] = '\0';

  printf("len of tmpStr: %u\n", (unsigned)strlen(tmpStr));
  printf("tmpStr: %s\n", tmpStr);

  for (int i=0; i< count; i++) {
     strcat(region, tmpStr);
     if ( i % 1000==0) {
        printf("len of region: %u\n", (unsigned)strlen(region));
     }
  }

  int unmap_result = munmap(region, 1 << 23);
  if (unmap_result != 0) {
    perror("Could not munmap");
    return 1;
  }
  // getpagesize
  return 0;
}
