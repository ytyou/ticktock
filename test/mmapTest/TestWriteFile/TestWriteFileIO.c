/**
 * Test how IO behaves when continuing append to a file.
 * start a tcollector to collect iostat metrics which shows how IO behaves.
 * The key metrics are iostat.disk.write_bytes, disk.util etc.
 */
#ifndef _GNU_SOURCE                     // Already defined by g++ 3.0 and higher.
#define _GNU_SOURCE 
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <malloc.h>

int main(void) {
  size_t pagecount = 1<<21;
  size_t pagesize = getpagesize();
  printf("System page size: %zu bytes\n", pagesize);

  const char* file_name="testWriteIO.txt";
  // Direct IO. Memory not affected.
  //int  fd = open(file_name, O_CREAT|O_RDWR|O_DIRECT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
  
  // normal writes. Mem.cached will go up, mem.free down.
  int  fd = open(file_name, O_CREAT|O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

  if (fd == -1){
        printf("Failed to open file %s", file_name);
        return 1;
  }

  long total_size=pagesize * pagecount - 1;
  printf("total_size=%ld\n", total_size);
 
  int fraction=32;
  const int len = pagesize/fraction; // try 128b write each time to see how io behaves
  // For DirectIO, align mem address
  //char *tmp_str = valloc(len);
  
  // For normal IO
  char *tmp_str = (char*)malloc(len);
  for(int i=0; i < len; i++) {
	tmp_str[i]='1';
  }
  tmp_str[len] = '\0';

  printf("len of tmp_str: %u\n", (unsigned)strlen(tmp_str));
  printf("tmpStr: %s\n", tmp_str);

  // allocate a large chunk of memory to simulate a system with memory pressure
  size_t tmp_str_page_count = pagecount /2;
  char* tmp_str_unused = (char*)calloc(pagesize, tmp_str_page_count);
  for(int i=0; i < tmp_str_page_count; i++) {
       memcpy(tmp_str_unused + i * pagesize, tmp_str, pagesize);
  }  
  tmp_str_unused[pagesize*tmp_str_page_count -1] = '\0';


  // Write len bytes 
  for (int j=0; j < pagecount*fraction; j++) {
         //int curr_page_index = page_index[j];

         write(fd, tmp_str, len);

         if ( j % 100000 ==0 ) {
            /*
             // Force flushing.
             if (msync(region, (unsigned)strlen(region), MS_SYNC) == -1) {
                 perror("Could not sync the file to disk");
             }
             */
             //printf("Len of region: %u\n", (unsigned)strlen(region));
             printf("Writes number: %u\n", j);
             fsync(fd);
         }    
         //sleep(0.01); // sleep for 1ms
  }

  close(fd);
  free(tmp_str);
  free(tmp_str_unused);
  return 0;
}
