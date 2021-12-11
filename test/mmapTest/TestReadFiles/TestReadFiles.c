#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>

int main(int argc, const char *argv[])
{    

    char filepath[100];
    printf("Enter file full path: ");
    scanf(" %100[^\n]", filepath);
    printf("Your file name is %s\n", filepath);
    
    //const char *filepath = "/tmp/mmapFile.tmp";

    int fd = open(filepath, O_RDONLY, (mode_t)0600);
    
    if (fd == -1)
    {
        perror("Error opening file for writing");
        exit(EXIT_FAILURE);
    }        
    
    struct stat fileInfo = {0};
    
    if (fstat(fd, &fileInfo) == -1)
    {
        perror("Error getting the file size");
        exit(EXIT_FAILURE);
    }
    
    if (fileInfo.st_size == 0)
    {
        fprintf(stderr, "Error: File is empty, nothing to do\n");
        exit(EXIT_FAILURE);
    }
    
    printf("File size is %ji\n", (intmax_t)fileInfo.st_size);
    
    char *map = mmap(0, fileInfo.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (map == MAP_FAILED)
    {
        close(fd);
        perror("Error mmapping the file");
        exit(EXIT_FAILURE);
    }
    
    /*
    for (off_t i = 0; i < fileInfo.st_size; i++)
    {
        printf("Found character %c at %ji\n", map[i], (intmax_t)i);
    }
    */
    
    printf("mmap len: %u\n",(unsigned)strlen(map));


    // Don't forget to free the mmapped memory
    if (munmap(map, fileInfo.st_size) == -1)
    {
        close(fd);
        perror("Error un-mmapping the file");
        exit(EXIT_FAILURE);
    }

    // Un-mmaping doesn't close the file, so we still need to do that.
    close(fd);
    
    return 0;
}
