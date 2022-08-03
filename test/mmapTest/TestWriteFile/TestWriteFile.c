/**
 * Test how IO behaves when continuing writing to mmapped files.
 * This program keeps writing to a mmaped file. We will also need to
 * start a tcollector to collect iostat metrics which shows how IO behaves.
 * The key metrics are iostat.disk.write_bytes, disk.util etc.
 */
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

int main(void) {
  int pagecount = 1<<19;
  size_t pagesize = getpagesize();
  printf("System page size: %zu bytes\n", pagesize);

  const char* file_name="testWriteMapped.txt";
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
  
  int rc = madvise(region, total_size, MADV_RANDOM);

  if (rc != 0)
        printf("Failed to madvise(RANDOM), page = %p", region);

  const int len = 64;
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

  for (int i=0; i < str_per_page; i++) {
     // Loop each page, write a string into it at i-th len position.
     // Thus, we can avoid appending strings to the file.
     for(int j=0; j < pagecount; j++) {
         //strcat(region, tmpStr); // strcat much slower than memcpy
         // Randomly write to different positions.
         memcpy(region + j*pagesize + i*len, tmp_str, len);

         if ( (i*pagecount + j) % 1000000 ==0 ) {
             /*
             // Force flushing.
             if (msync(region, (unsigned)strlen(region), MS_SYNC) == -1) {
                 perror("Could not sync the file to disk");
             }
             */
             printf("Len of region: %u\n", (unsigned)strlen(region));
         }    
         sleep(0.001); // sleep for 1ms
     }
  }

  int unmap_result = munmap(region, total_size);
  if (unmap_result != 0) {
    perror("Could not munmap");
    close(fd);
    return 1;
  }

  close(fd);
  return 0;
}
