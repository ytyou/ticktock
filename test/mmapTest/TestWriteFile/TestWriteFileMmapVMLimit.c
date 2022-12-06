/**
 * Repeatly create mmap files to see how many files allowed in 32bit/64bit VM spaces.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <vector>

typedef struct FdMmapItem {
   int fd;   
   char* region;
} FMItem;

FMItem* create_mmap_file(size_t total_size, int index) {
  int fn_len = 13;
  char file_name[fn_len]; // len(file_name_prefix) + 4 + '\0';
  snprintf(file_name, fn_len, "tmp_mmap.%d", index);

  int  fd = open(file_name, O_CREAT|O_RDWR, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);

  if (fd == -1){
        printf("Failed to open file %s", file_name);
        return NULL;
  }

  // Stretch the file size to the size of the (mmapped) array of char.
  // Go back 1 byte to write \0. 
    if (lseek(fd, total_size-1, SEEK_SET) == -1)
    {
        close(fd);
        perror("Error calling lseek() to 'stretch' the file");
        return NULL;
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
        return NULL;
    }
  
  char * region = (char*) mmap(
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
    close(fd);
    return NULL;
  }

  FMItem *item = (FMItem*) malloc(sizeof(FMItem));
  item->fd = fd; 
  item->region = region; 
  
  printf("Successfully mmapped file %s with %zu bytes\n", file_name, total_size);

  return item;
}

// ./a.out <num mmap files> <page count of power of 2, e.g, 14 means 2^14=64MB>
int main(int argc, char *argv[]) {
  if (argc != 3) {
     printf("Arguments must be 2.\ne.g., ./a.out <num mmap files> <page count of power of 2, e.g, 14 means 2^14=64MB>\n");
     return -1;
  }

  // number of mmapped files
  int max_mmap_num = atoi(argv[1]);

  // e.g., argv[2] = 14, pagecount = 2^14.
  size_t pagecount = 1<< atoi(argv[2]);
  size_t pagesize = getpagesize();
  size_t total_size = pagecount * pagesize;
  printf("System page size: %zu bytes, total_size: %zu\n", pagesize, total_size);

  std::vector<FMItem*> mmaps;
  mmaps.reserve(max_mmap_num);
 
  for(int i=0; i < max_mmap_num; i++) {
      FMItem* fdItem = create_mmap_file(total_size, i);
      mmaps.push_back(fdItem);
      sleep(5);
  }

  printf("Successfully mmapped %d files with %zu bytes\n", max_mmap_num, total_size);
 
  for(auto it=mmaps.begin(); it != mmaps.end(); it++) {
      auto i = std::distance(mmaps.begin(), it); 
      int unmap_result = munmap((*it)->region, total_size);
      if (unmap_result != 0) {
          printf("munmap fail at %d\n!", i);
      }

      close((*it)->fd);
      free(*it);
  }
  mmaps.clear();

  return 0;
}
