from urlparse import urlparse
import re

url="http://calendar.ics.uci.edu/calendar.php?type=month&calendar=1&category=&month=03&year=2017"

parsed=urlparse(url)

if len(parsed.path) > 0:
	data = str(parsed.path)
	if len(parsed.query) > 0:
		data = str(data) + str(parsed.query)
	if len(parsed.fragment) > 0:
		data =str(data) + str(parsed.fragment)
	list_words = re.sub(r"\W+"," ",data).split(" ")
	c= collections.Counter(list_words)
	print c