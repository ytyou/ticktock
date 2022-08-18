/**
 * Test how IO behaves when continuing writing to mmapped files.
 * This program keeps writing to a mmaped file. We will also need to
 * start a tcollector to collect iostat metrics which shows how IO behaves.
 * The key metrics are iostat.disk.write_bytes, disk.util etc.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

void swap(size_t *a, size_t *b) {
  size_t tmp = *a;
  *a = *b;
  *b = tmp;
}

void randomize(size_t* array, size_t size) {
  srand(time(0)); // seeding
  
  for(size_t i=0; i < size; i++) {
      size_t random_index = rand() % (size-i);
      swap(array+i, array+i + random_index);
  }
}

int main(void) {
  size_t pagecount = 1<<20;
  size_t pagesize = getpagesize();
  printf("System page size: %zu bytes\n", pagesize);
  // initialize an array with in-order indexes
  size_t* page_index = (size_t*)malloc( pagecount * sizeof(size_t));
  for(size_t i=0; i < pagecount; i++) {
      page_index[i] = i;
  }
  
  // randomize it. Comment out if necessary
  //randomize(page_index, pagecount);

  const char* file_name="testWriteMapped.txt.append";
  int  fd = open(file_name, O_CREAT|O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

  if (fd == -1){
        printf("Failed to open file %s", file_name);
        return 1;
  }

  long total_size=pagesize * pagecount - 1;
  printf("total_size=%ld\n", total_size);

  // Stretch the file size to the size of the (mmapped) array of char.
  // Go back 1 byte to write \0. 
    if (lseek(fd, total_size-1, SEEK_SET) == -1)
    {
        close(fd);
        perror("Error calling lseek() to 'stretch' the file");
        return 1;
    }
    
    /* Something needs to be written at the end of the file to
     * have the file actually have the new size.
     * Just writing an empty string at the current file position will do.
     *
     * Note:
     *  - The current position in the file is at the end of the stretched 
     *    file due to the call to lseek().
     *  - An empty string is actually a single '\0' character, so a zero-byte
     *    will be written at the last byte of the file.
     */
    
    if (write(fd, "", 1) == -1)
    {
        close(fd);
        perror("Error writing last byte of the file");
        return 1;
    }
  
  char * region = mmap(
  //char * region = mmap64(
    0,   // Map from the start of the 0th page
    total_size,   // expect 4K * 1M / 2 = 2GB
    PROT_READ|PROT_WRITE|PROT_EXEC,
    MAP_SHARED,
    fd,
    0
  );

  if (region == MAP_FAILED) {
    perror("Could not mmap");
    return 1;
  }

/*  
  int rc = madvise(region, total_size, MADV_RANDOM);

  if (rc != 0)
        printf("Failed to madvise(RANDOM), page = %p", region);
*/
  const int len = pagesize;
  char tmp_str[len];
  for(int i=0; i < len; i++) {
	tmp_str[i]='1';
  }
  tmp_str[len] = '\0';

  printf("len of tmp_str: %u\n", (unsigned)strlen(tmp_str));
  printf("tmpStr: %s\n", tmp_str);

  long str_count = total_size / len;
  long str_per_page = pagesize / len;
  printf("str_count=%ld, str_per_page=%ld\n", str_count, str_per_page);

  int tmp_loop = 20;
  //for (int i=0; i < str_per_page; i++) {
     // Loop each page, write a string into it at i-th len position.
     // Thus, we can avoid appending strings to the file.
     
     for (int j=0; j < pagecount * tmp_loop; j++) {
         int curr_page_index = page_index[j%pagecount];

         //strcat(region, tmpStr); // strcat much slower than memcpy
         // Randomly write to different positions.
         memcpy(region + curr_page_index * pagesize, tmp_str, len);

         if ( j % 100000 ==0 ) {
             /*
             // Force flushing.
             if (msync(region, (unsigned)strlen(region), MS_SYNC) == -1) {
                 perror("Could not sync the file to disk");
             }
             */
             printf("Len of region: %u\n", (unsigned)strlen(region));
         }    
         sleep(0.01); // sleep for 1ms
     }
  //}

  int unmap_result = munmap(region, total_size);
  if (unmap_result != 0) {
    perror("Could not munmap");
    close(fd);
    free(page_index);
 
    return 1;
  }

  close(fd);
  free(page_index);
  return 0;
}