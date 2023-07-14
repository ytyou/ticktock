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

    ============================================================
    This program reads opentsdb put requests (one in each line) from stdin, and sends to TT in TCP.

    Compile with: g++ single_client_tcp.cpp -o single_client_tcp

    run:
    [Yi-MBP tools (migrate)]$ echo "put testM1 1633412175000 123 host=foo" | ./single_client_tcp -h 192.168.1.41 -p 6181
 */

#include <iostream>
#include <stdio.h>
#include <string>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

std::string g_host;
int g_tcp_port=6181;

int
connect_to_host()
{
    int fd, retval;
    struct sockaddr_in addr;

    fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd == -1)
    {
        printf("socket() failed, errno = %d\n", errno);
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(g_tcp_port);
    inet_pton(AF_INET, g_host.c_str(), &addr.sin_addr.s_addr);

    retval = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (fd == -1)
    {
        printf("connect() failed, errno = %d\n", errno);
        return -1;
    }

    return fd;
}

int
tcp_send(int fd, const char *body, int len)
{
    int sent_total = 0;

    while (len > 0)
    {
        int sent = send(fd, body+sent_total, len, 0);

        if (sent == -1)
        {
            fprintf(stderr, "tcp_send %s failed: %d\n", body, errno);
            return 1;
        }

        len -= sent;
        sent_total += sent;
    }

    std::cout << body<< " " << sent_total << "bytes sent\n";

    return 0;
}

static int
process_cmdline_opts(int argc, char *argv[])
{
	const char* usage = "Usage: %s [-h <host, default localhost>] [-p <tcp port, default 6181>]\n";

	if (argc > 5)
	{
        fprintf(stderr, usage, argv[0]);
        return 2;
	}

    int c;

    while ((c = getopt(argc, argv, "?h:p:")) != -1)
    {
        switch (c)
        {
            case 'h':
            	// target TT host
            	g_host.assign(optarg);
            	break;

            case 'p':
            	// target TT tcp port
            	g_tcp_port = atoi(optarg);
                break;
            case '?':
                fprintf(stderr, usage, argv[0]);

                return 1;
            default:
            	std::cout <<" 2--"<<c<<"\n";
            	fprintf(stderr, usage, argv[0]);

                return 2;
        }
    }

    if (g_host.empty())
    {
    	g_host.assign("localhost");
    }

    return 0;
}

int main(int argc, char* args[])
{
	if (process_cmdline_opts(argc, args) != 0)
	{
		return 1;
	}

	// connect to host
	int fd = connect_to_host();

	std::string line;
	// loop: read from stdin and send to TT
	// Each line is supposed to be a Opentsdb PUT.
	while(std::getline(std::cin, line))
	{
		tcp_send(fd, line.c_str(), line.length());
	}
	return 0;
}
