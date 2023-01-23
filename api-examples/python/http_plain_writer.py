import sys
import requests

PY3 = sys.version_info[0] > 2
if PY3:
    from urllib.request import Request, urlopen # pylint: disable=maybe-no-member,no-name-in-module,import-error
    from urllib.error import HTTPError, URLError # pylint: disable=maybe-no-member,no-name-in-module,import-error
else:
    from urllib2 import Request, urlopen, HTTPError, URLError # pylint: disable=maybe-no-member,no-name-in-module,import-error

if len(sys.argv) != 3:
    print("Usage: python http_plain_writer.py <host> <port>")
    sys.exit(1);
else:
    HOST = sys.argv[1]
    PORT = sys.argv[2]

try:
    url = "http://%s:%s/api/put" % (HOST, PORT)
    req = Request(url)    
    req.add_header('Content-type', 'application/text')

    put_data_point1 = 'put http.cpu.usr 1633412175 20 host=foo cpu=1';
    put_data_point2 = 'put http.cpu.sys 1633412175 20 host=foo cpu=1';
    put_req = put_data_point1 +'\n'+put_data_point2+'\n';

    response = urlopen(req, put_req.encode())
    print("Received response:", response.getcode()) 

    print("To query:");
    url = "http://%s:%s/api/query?start=1633412100&m=avg:1m-avg:http.cpu.usr{host=foo}" % (HOST, PORT)
    res = requests.get(url)
    print(res.text)
    print(res.url)
except HTTPError as e1:
    print("HTTP Exception:", e1)
except URLError as e2:
    print("URL Exception:", e2)

