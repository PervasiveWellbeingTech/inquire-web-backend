"""
This is the parser for the XML dump, which will output JSON.
"""
import json
import os
import logging
from xml.etree.cElementTree import iterparse
import base64
import multiprocessing
import xml
import html
from html.parser import HTMLParser

import itertools

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(name)-18s: %(message)s",
    level=logging.DEBUG
)

n_cpus = multiprocessing.cpu_count()

log = logging.getLogger(__name__)
DATA_ROOT = "/commuter/raw_data_livejournal/data"

current_file, current_post_num = 0, 0


def iterate_raw_filenames():
    events_folder = os.path.join(DATA_ROOT, "events")
    events_content = os.listdir(events_folder)
    for subfolder in events_content:
        subfolder_path = os.path.join(events_folder, subfolder)
        subfolder_content = os.listdir(subfolder_path)
        yield from (os.path.join(subfolder_path, name) for name in subfolder_content if name.endswith(".xml"))

# EXAMPLE
# ('user', 'gbwpsweetie7'),
# ('itemid', '2'),
# ('subject', 'good morning'),
# ('event', '<FONT color=#ffccff><FONT size=4><STRONG>good morning yall. whats up? not a lot here jsut woke up an thought i would write. so yeah last night i was talkin to i like a lot an i want to go out with him but i cant. i like bieng sinlge. and he just cant understand that. he is starting to talk to this other gurl and he is starting to be an asshole to me an di kinda dont want that to happen AGAIN! so yeah so if you guys would, would yall leave a comment about what i should do, please? thank you i can really use yer help!!!! love katie</STRONG> </FONT></FONT>'),
# ('ditemid', '734'),
# ('eventtime', '2005-04-30 08:31:00'),
# ('props', 'really jealous1331114964338Cold by Crossfade111114865473'),
# ('logtime', '2005-04-30 12:43:02'),
# ('anum', '222'),
# ('url', 'http://gbwpsweetie7.livejournal.com/734.html'),
# ('event_timestamp', '1114849860'),
# ('reply_count', '1')
keep = {"user", "itemid", "ditemid", "subject", "event", "eventtime", "reply_count"}
cast = {"itemid": int, "ditemid": int, "reply_count": int}


def handle_post(elem):
    itemdict = {}
    for child in elem.getchildren():
        if child.tag not in keep:
            continue
        try:
            content = next(child.itertext())
        except StopIteration as e:
            # print(child)
            raise e
        if child.tag == "itemid" or child.tag == "ditemid":
            itemdict[child.tag] = int(content)
        elif child.tag == "event" or child.tag == "subject":
            child_child = child.getchildren()[0]
            if child_child.tag == "string":
                text = content
            elif child_child.tag == "base64":
                try:
                    text = base64.b64decode(content).decode('utf-8')
                except UnicodeDecodeError as e:
                    return None
            else:
                # print(list(elem.itertext()))
                # print("Found child %s in event!" % child_child.tag)
                text = content
            try:
                text = strip_tags(text)
            except RuntimeWarning:
                log.debug("discarding %s" % itemdict)
                return None
            if child.tag == "event":
                itemdict["text"] = text
            else:
                itemdict["subject"] = text
        else:
            itemdict[child.tag] = content
    return itemdict


def handle_filen(filename):
    global current_post_num
    current_post_num = 0
    file_buf = []
    with open(filename, "rb") as inf:
        # get an iterable
        try:
            context = iterparse(inf, events=("end",))
            event, root = next(context)
            for event, elem in context:
                if event == "end" and elem.tag == "post":
                    r = handle_post(elem)
                    if r is not None:
                        file_buf.append(r)
                        current_post_num += 1
                    root.clear()
        except xml.etree.ElementTree.ParseError as e:
            log.exception("Error occurred!")
            log.debug(e)
            log.debug("File was %s. Skipping rest of this file." % filename)
            log.debug("filebuffer len is %s " % len(file_buf))
    return file_buf


class MLStripper(HTMLParser):
    def __init__(self):
        super().__init__()
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)
    def error(self, message):
        log.debug(message)
        raise RuntimeWarning("Error occurred in HTML parser: %s" % message)


def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()


def clean_text(text_raw):
    html_unescaped = html.unescape(text_raw)
    return strip_tags(html_unescaped)


def grouper(n, iterable):
    while True:
        chunk = tuple(itertools.islice(iterable, n))
        if not chunk:
            break
        yield chunk


def run_parse(out_path, limit_files=None):
    global current_file, current_post_num
    if limit_files is None:
        num_files = 0
        for _ in iterate_raw_filenames():
            num_files += 1
    else:
        num_files = limit_files
    log.debug("Will parse %s files.." % num_files)
    i = 0
    chunksize = 20000 if limit_files is None else min(limit_files, 20000)
    with multiprocessing.Pool(processes=n_cpus) as pool:
        with open(out_path, "w") as outf:
            for filen_chunk in grouper(chunksize, iterate_raw_filenames()):
                current_file += chunksize
                log.debug("New chunk of %s documents.. (%s/%s so far)" % (chunksize, i, num_files))
                try:
                    res = list(pool.map(handle_filen, filen_chunk))
                except Exception as e:
                    log.error("Error occurred. file = %s, post = %s." % (current_file, current_post_num))
                    raise e
                for fname, r in zip(filen_chunk, res):
                    if not r:
                        # print("%s is empty" % fname)
                        continue
                    for post in r:
                        outf.write(json.dumps(post))
                        outf.write("\n")
                i += len(filen_chunk)
                if i >= num_files:
                    break


if __name__ == '__main__':
    # handle_filen("/commuter/raw_data_livejournal/data/events/gb/gbnmf72400.xml")
    run_parse("/commuter/full_lj_parse_philipp.json", limit_files=None)
