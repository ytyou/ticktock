// g++ TestAppendMeta.c
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <string>

int main(void) {
    std::string m_name="ticktock.tmp.meta";

    //int fd = ::open(m_name.c_str(), O_CREAT|O_WRONLY|O_NONBLOCK, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    int fd = ::open(m_name.c_str(), O_WRONLY|O_CREAT|O_APPEND|O_DSYNC, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
    
    if (fd == -1)
    {
        printf("Failed to open file %s for append: %d", m_name.c_str(), errno);
        exit(1);
    }
    
    std::FILE *m_file = fdopen(fd, "a");

    if (m_file == nullptr)
    {
       printf("Failed to convert fd %d to FILE: %d", fd, errno);
       exit(1);
    }

    // Append 1000*1000 lines to m_file
    for(int i=0; i<1000; i++) {
       for(int j=0; j<1000; j++) {
           fprintf(m_file, "g_1 device=%u; sensor=%u; %u\n", i, j, i*j);
       }
       printf("Done with i=%u * 1000\n", i);
    }
    
    std::fclose(m_file);
    m_file=nullptr;
    return 0;
}

