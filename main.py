import re
import datetime
import subprocess
import shlex
import pandas as pd


def file_names():

    """
    A function which will return filepaths for current date.
    :return: filepaths for shell script, joblog, and unpacker
    """

    # directory path
    directory = '/home/corpus/'

    # use datetime to get the current date then change it into filepath format
    current_date = datetime.date.today()
    format = 'unpacking-' + current_date.strftime('%Y-%m-%d')

    # create the filepath variables
    shell_filepath = directory + format + '.sh'
    joblog_filepath = directory + format + '.joblog'
    unpacker_filepath = directory + 'unpackernodes'

    return shell_filepath, joblog_filepath, unpacker_filepath


def read_file(fn):

    """
    Just reads the file :)
    :param fn: filename.  Just the filepath
    :return: the contents of the file
    """

    # open the file and read it line by line
    with open(fn) as f:
        file_data = f.readlines()

    # return the file contents
    return file_data


def shell_ids(fp):

    """
    This function finds the ETIDs and object IDs, used for shell script
    :param fp: file path
    :return: etid list and object id list
    """

    dn = read_file(fp)

    # these are the regex patterns for ETIDs and object IDs, respectively
    etid_pattern = re.compile(r'(-E\s\d{1,})')
    obj_pattern = re.compile(r'(-o\s\d{1,})')

    # for each element in the list created from readlines()
    for i in range(0, len(dn)):

        # create empty lists for etids and object ids
        etids = []
        objids = []

        # use regex to remove the newlines and find etids!
        dn[i] = re.sub('\n', '', dn[i])
        data_etid = ''.join(etid_pattern.findall(dn[i]))

        # if there is an ETID, then add it to list of etids created earlier, if not find object id and add to list
        if len(data_etid) != 0:
            etids.append(data_etid[3:])
        else:
            obj_id = ''.join(obj_pattern.findall(dn[i]))
            objids.append(obj_id[3:])

    return etids, objids


def joblog_process(fp):
    """
    Full process for the joblog file.
    :param fp: joblog file filepath
    :return: list of etids and objids in joblog file as well as tuples with their id, start time,
    """

    data = read_file(fp)
    data.pop(0)

    # function uses etid/objid, job start time, job run time, and the current ip to create a tuple
    def time_tuple(id, job_start, job_run, current_ip):

        job_run = float(job_run)
        job_start = float(job_start)

        # find job finish time
        job_finish = int(job_run + job_start)
        job_start = int(float(job_start))

        # convert to understandable times
        fin_start = datetime.datetime.fromtimestamp(job_start).strftime('%H:%M %Y-%m-%d')
        fin_end = datetime.datetime.fromtimestamp(job_finish).strftime('%H:%M %Y-%m-%d')

        # create the tuple
        new_tuple = (id, 'Done', fin_start, fin_end, job_run, ' ', current_ip)

        return new_tuple

    # use regular expressions to get the times
    def get_times(line):

        # various patterns.  pre_pattern is to take out the part in front of what we want
        job_start_pattern = re.compile(r'^\d{1,}\s{1,}[a-z]{1,}@\d{1,}.\d{1,}.\d{1,}.\d{1,}\s\d{1,}.\d{1,}')
        start_pre_pattern = re.compile(r'^\d{1,}\s{1,}[a-z]{1,}@\d{1,}.\d{1,}.\d{1,}.\d{1,}\s')
        job_run_pattern = re.compile(r'^\d{1,}\s{1,}[a-z]{1,}@\d{1,}.\d{1,}.\d{1,}.\d{1,}\s\d{1,}.\d{1,}\s{1,}\d{1,}.\d{1,}')
        run_pre_pattern = re.compile(r'^\d{1,}\s{1,}[a-z]{1,}@\d{1,}.\d{1,}.\d{1,}.\d{1,}\s\d{1,}.\d{1,}\s{1,}')

        # find start time, then take out the pre pattern bit
        start_time = ''.join(job_start_pattern.findall(line))
        start_time = re.sub(start_pre_pattern, '', start_time)

        # find run time, then take out the pre pattern bit
        run_time = ''.join(job_run_pattern.findall(line))
        run_time = re.sub(run_pre_pattern, '', run_time)

        return start_time, run_time



    # patterns for finding etids, object ids, and IPs
    etid_pattern = re.compile(r'(-E\s\d{1,})')
    obj_pattern = re.compile(r'(-o\s\d{1,})')
    ip_pattern = re.compile(r'^\d{1,}\s{1,}[a-zA-Z]{1,}[^a-z](\d{1,}.){4}\s{1,}')

    # create empty lists for etids, object ids, and time triples
    joblog_etids = []
    joblog_objids = []
    time_tuples = []

    # for each element in the list created from readlines()
    for i in range(0, len(data)):

        line = data[i]

        # use regex to remove the newlines and find etids!
        line = re.sub('\n', '', line)
        data_etid = ''.join(etid_pattern.findall(line))
        fin_etid = data_etid[3:]
        data_ip = ''.join(ip_pattern.findall(line))
        current_ip = data_ip[-2:]

        # if there is an ETID, then add it to list of etids created earlier, if not find object id and add to list
        if len(data_etid) != 0:
            joblog_etids.append(fin_etid)

            etid_start, etid_run = get_times(line)
            etid_tuple = time_tuple(fin_etid, etid_start, etid_run, current_ip)
            time_tuples.append(etid_tuple)

        else:
            obj_id = ''.join(obj_pattern.findall(line))
            fin_objid = obj_id[3:]
            joblog_objids.append(fin_objid)

            objid_start, objid_run = get_times(line)
            objid_tuple = time_tuple(fin_objid, objid_start, objid_run, current_ip)
            time_tuples.append(objid_tuple)


    return joblog_etids, joblog_objids, time_tuples


def find_nodes(fp):

    """
    Function reads the unpacker nodes
    :param fp: filepath name of unpacker nodes file
    :return: # of cores, ip addresses, dict of both
    """

    dn = read_file(fp)

    # the regex pattern to find number of cores!
    core_pattern = re.compile(r'(^\d{1,})')

    # empty list to store numbers for cores
    all_cores = []

    # for each line of the unpackernodes file find core numbers and add to list
    for i in range(0, len(dn)):
        core = ''.join(core_pattern.findall(dn[i]))
        all_cores.append(int(core))

    # sum all cores and place in new list
    sum_cores = [sum(all_cores)]

    # the regex pattern to find full ip address
    ip_pattern = re.compile(r'(\d{1,}.\d{1,}.\d{1,}.\d{1,})')

    # empty list to store ips
    ips = []

    # for each line of file find ip address, take last two numbers, add to list
    for i in range(0, len(dn)):
        ip = ''.join(ip_pattern.findall(dn[i]))
        ip = ip[-2:]
        ips.append(ip)

    # create a dictionary with the ips as the key
    dict = {x:0 for x in ips}

    # get core then set it as its IP's value in the dictionary
    for i in range(0, len(dn)):
        core = ''.join(core_pattern.findall(dn[i]))
        ip = ''.join(ip_pattern.findall(dn[i]))
        ip = ip[-2:]

        dict[ip] = core

    return sum_cores, ips, dict


def bash_subprocess():

    """
    Runs the bash command in a subprocess and parses the output
    :return: list of tuples with ip address and ETID/object id being worked on, list of etids, and list of object ids
    """

    # run the bash subprocess and split them based on ## so that they're split according to IP
    bash_command = """grep '\.' unpackernodes | awk -F'\.' '{print $(NF)}' | xargs -d $'\n' sh -c 'for arg do echo "## $arg"; ssh 192.168.58.$arg ps -ax | grep ETID;  done' _"""
    split_args = shlex.split(bash_command)
    bash_process = subprocess.Popen(split_args, stdout=subprocess.PIPE)
    bash_output = bash_process.stdout.read()
    bash_chunks = bash_output.split('##')

    # empty lists to store the subprocess output tuples, etids, and object IDs
    ps_tuples = []
    ps_etids = []
    ps_objids = []

    # regular expression patterns.  final_id is purely the id whereas ps_id_pattern includes the -E/-o
    ps_id_pattern = re.compile(r'-[a-zA-Z]\s\d{1,}$')
    final_id = re.compile(r'\d{1,}$')
    pid_pattern = re.compile(r'^\s*\d{1,}')

    # for each of the earlier chunks we split
    for chunk in bash_chunks:

        # all of the empty ip lines have a length of 7
        if len(chunk) == 7:
            bash_chunks.remove(chunk)

        # but if the length is not 7!! then there are processes :)
        else:

            # split them on the newline
            id_chunks = chunk.split('\n')
            # this just removes empty chunk
            id_chunks = [line for line in id_chunks if line]

            # if there are two because it's a little wonky with the else code
            if len(id_chunks) == 2:

                ip_ad = id_chunks[0]

                # get the actual ETID or object ID with final_id pattern
                ps_id = ''.join(final_id.findall(id_chunks[1]))
                # get the pid
                pid = ''.join(pid_pattern.findall(id_chunks[1]))
                pid = pid.strip()
                # make a tuple out of it and append!
                new_tuple = (ps_id, 'Running', ' ', ' ', ' ', pid, ip_ad[-2:])
                ps_tuples.append(new_tuple)

                # get the ETID/object ID along with the -E/-o
                type_chunk = ''.join(ps_id_pattern.findall(id_chunks[1]))
                # if it's -E then add it to ETID list.  if it's -o add to object ID list
                if type_chunk.startswith('-E'):
                    ps_etids.append(ps_id)
                elif type_chunk.startswith('-o'):
                    ps_objids.append(ps_id)

            # for all chunks that have more than two running
            else:

                for x in range(1, len(id_chunks)):

                    ip_ad = id_chunks[0]

                    # get actual ETID/object ID with final_id_pattern
                    ps_id = ''.join(final_id.findall(id_chunks[x]))
                    # get the pid with pid_pattern
                    pid = ''.join(pid_pattern.findall(id_chunks[x]))
                    # make tuple and append
                    new_tuple = (ps_id, 'Running', ' ', ' ', ' ', pid, ip_ad[-2:])
                    ps_tuples.append(new_tuple)

                    # get the ETID/object ID along with the -E/-o
                    type_chunk = ''.join(ps_id_pattern.findall(id_chunks[x]))
                    # if it's -E, add to list of ETIDs.  if it's -o add to list of object IDs
                    if type_chunk.startswith('-E'):
                        ps_etids.append(ps_id)
                    elif type_chunk.startswith('-o'):
                        ps_objids.append(ps_id)

    return ps_tuples, ps_etids, ps_objids


def sort_etids(shell_etids, shell_objids, joblog_etids, joblog_objids, bash_etids, bash_objids):

    """
    This function subtracts the finished and running ETIDs/object IDs from the shell list of all
    ETIDs/object IDs to find the ones which are still waiting for work.  Also finds
    the ETIDs and object IDs which show up in joblog/bash output but not in the shell
    file, which is cause for concern.

    :param shell_etids: ETIDs from the shell file
    :param shell_objids: object IDs from the shell file
    :param joblog_etids: ETIDs from the joblog file
    :param joblog_objids: object IDs from the joblog file
    :param bash_etids:  ETIDs from the bash output file
    :param bash_objids: object IDs from the bash output file
    :return: list of waiting ETIDs, waiting object IDs, concerning IDs, and concerning object IDs
    """

    # make lists of waiting ETIDs and waiting object IDs
    waiting_etids = [etid for etid in shell_etids if etid not in joblog_etids and etid not in bash_etids]
    waiting_objids = [objid for objid in shell_objids if objid not in joblog_objids and objid not in bash_objids]

    # make lists of ETIDs and object IDs which are very large
    large_etids = [etid for etid in bash_etids if etid not in shell_etids]
    large_objids = [objid for objid in bash_objids if objid not in shell_objids]

    # make lists of ETIDs and object IDs which are sync concerns
    concern_etids = [etid for etid in joblog_etids if etid not in shell_etids]
    concern_objids = [objid for objid in joblog_objids if objid not in shell_objids]

    return waiting_etids, waiting_objids, concern_etids, concern_objids, large_etids, large_objids


def create_shell_tuples(waiting_etids, waiting_objids):

    """
    This function creates tuples for the ETIDs and object IDs in the waiting lists.
    :param waiting_etids: list of waiting ETIDs from sort_etids()
    :param waiting_objids: list of waiting object IDs from sort_etids()
    :return: shell script tuples with the waiting ETIDs and object IDs
    """

    # add them together
    merged_ids = waiting_etids + waiting_objids

    # create empty list of tuples
    shell_tuples = []

    # create tuples and append to list
    for id in merged_ids:

        new_tuple = (id,'Waiting',' ',' ',' ',' ',' ')
        shell_tuples.append(new_tuple)

    return shell_tuples


def ip_table(ps_tuples,ips,sum_cores,nodes_dict):

    """
    This function creates the full html, including header, for the table showing running
    CPUs and IPs.

    :param ps_tuples: list of tuples from the subprocess output
    :param ips: list of all IPs
    :param sum_cores: sum of the possible cores
    :param nodes_dict: dictionary of IPs and running cores
    :return: html for table and header
    """

    # create dictionary with empty values and IPs as keys
    ip_dict = {x:0 for x in ips}

    # increase num of cores running for each IP by one for each corresponding tuple
    for i in ps_tuples:
        etid_ip = i[6]
        ip_dict[etid_ip] += 1

    # create list of the IPs and insert 'IPs' at the beginning
    ip_list = list(ip_dict.keys())
    ip_list.insert(0,'IPs')

    # create list of the running cores
    running_ips = list(ip_dict.values())
    # get their sum so we can say how many are in use
    sum_running = sum(running_ips)
    # insert 'Running' at the beginning
    running_ips.insert(0, 'Running')

    # create list of all the cores and insert 'CPUs' at the beginning
    cores_list = list(nodes_dict.values())
    cores_list.insert(0, 'CPUs')

    # add all the lists together, convert to dataframe, fix columns
    ip_table_data = [ip_list, cores_list, running_ips]
    ip_df = pd.DataFrame(ip_table_data)
    ip_df.columns = ip_df.iloc[0]
    ip_df.drop(0,axis=0,inplace=True)

    # convert dataframe to a table
    ip_table_html = ip_df.to_html()

    sum_cores_html = "<b>" + str(sum_cores[0]) + "</b>"
    sum_running_html = "<b>" + str(sum_running) + "</b>"

    # write header
    begin_html = """<h2>CPU USE</h2><p>{CPU} CPUs available.  {CPU_USE} in use.</p>""".format(CPU=sum_cores_html,CPU_USE=sum_running_html)

    # add header to table html
    ip_table_html = begin_html + ip_table_html

    return ip_table_html


def status_table(shell_tuples,joblog_tuples,bash_tuples):

    """
    This function creates the full html for the ETID status table including header.
    :param shell_tuples: tuples from the shell script file
    :param joblog_tuples: tuples from the joblog file
    :param bash_tuples: tuples from the bash output file
    :return: html of ETID status table header and table
    """

    # add all the tuples together and create a dataframe with columns
    table_data = shell_tuples + joblog_tuples + bash_tuples
    table_df = pd.DataFrame(table_data, columns=['ETID','Status','Start Time','Finish Time','Run Seconds','PID','IP'])
    table_df.index = table_df.index + 1

    # convert dataframe to html
    table_html = table_df.to_html()

    # write header
    begin_html = """<h2>ETID STATUS</h2>"""

    # add header to table html
    table_html = begin_html + table_html

    return table_html


def main():

    # get the file names
    shell_fp, joblog_fp, unpacker_fp = file_names()

    # create empty list of errors
    error_list = []

    try:
        shell_etids, shell_objids = shell_ids(shell_fp)
    except FileNotFoundError:
        error_list.append("Shell file not found.")
    except TypeError:
        error_list.append("Shell file not in correct format.")

    try:
        joblog_etids, joblog_objids, time_tuples = joblog_process(joblog_fp)
    except FileNotFoundError:
        error_list.append("Joblog file not found.")
    except TypeError:
        error_list.append("Joblog file not in correct format.")

    try:
        sum_cores, ips, nodes_dict = find_nodes(unpacker_fp)
    except FileNotFoundError:
        error_list.append("Unpacker nodes file not found.")
    except TypeError:
        error_list.append("Unpacker nodes file not in correct format.")

    try:
        ps_tuples, ps_etids, ps_objids = bash_subprocess()
    except subprocess.CalledProcessError as e:
        error_list.append(str(e.message))
    except TypeError:
        error_list.append("PS output file not in correct format.")

    if len(error_list) != 0:
        # if there is an error then do not go into the rest of the script

        # early html/later html
        head_errorHTML = """<html><head><link href="style.css" rel="stylesheet" type="text/css" /><title>{title}</title></head><body><h1><concern>ERROR</concern></h1><p>The unpacker dashboard ran into the following errors while trying to execute: </p>"""
        end_errorHTML = """</body></html>"""

        title = 'NSRL Unpacker Dashboard'

        # creating a list with each element from error_list
        list_html = "<ul>"

        for error_found in error_list:
            error_html = "<li>" + error_found + "</li>"
            list_html = list_html + error_html

        list_html = list_html + "</ul>"

        # combine it all!
        error_html = head_errorHTML.format(title=title) + list_html + end_errorHTML
        print(error_html)

    else:
        # if there are no errors, we can proceed!

        waiting_etids, waiting_objids, concern_etids, concern_objids, large_etids, large_objids = sort_etids(
                shell_etids, shell_objids, joblog_etids, joblog_objids, ps_etids, ps_objids)

        shell_tuples = create_shell_tuples(waiting_etids, waiting_objids)

        table_html = status_table(shell_tuples,time_tuples,ps_tuples)

        ip_table_html = ip_table(ps_tuples, ips, sum_cores, nodes_dict)

        # variables for formatting HTML later
        title = 'NSRL Unpacker Dashboard'
        description_text = 'This is the NSRL Unpacker Dashboard! <running>Running</running> means the ETID is ' \
                           'currently running, <done>Done</done> means that the ETID is finished running, ' \
                           'and <waiting>Waiting</waiting> means that the ETID has not yet started the unpacking ' \
                           'process.  If the ETID is highlighted in <concern>this color</concern>, then that means ' \
                           'the ETID is in the joblog file despite not being in the ' \
                           'shell script file, meaning there may be an issue with the ETID.  ' \
                           'If the ETID is highlighted in <large>this color</large>, then that means' \
                           'the ETID is in the bash output file without being in the shell file, meaning it may be' \
                           'a very large file.'

        # beginning HTML before the tables
        html_beginning = """<HTML>
          <HEAD>
            <title>{title}</title>
            <link rel="stylesheet" type="text/css" href="mystyle.css">
          </HEAD>
    
          <BODY>
            <h1>{title}</h1>
            <p>{description}</p>
            """

        # ending html after all the tables
        html_ending = """</BODY></HTML>"""

        # pattern so I can put in the style tags
        waiting_pattern = re.compile(r'Waiting')
        done_pattern = re.compile(r'Done')
        running_pattern = re.compile(r'Running')

        # merging concern ETIDs and object IDs
        merged_concern = concern_etids + concern_objids

        # put the concern tag around them for their text color
        for i in merged_concern:
            concern_pattern = re.compile(r"{i}")
            table_html = re.sub(concern_pattern,"""<concern>{i}</concern>""", table_html)

        # merging large ETIDs and object IDs
        merged_large = large_etids + large_objids

        # put the large tag around them for their text color
        for i in merged_large:
            large_pattern = re.compile(r"{i}")
            table_html = re.sub(large_pattern,"""<large>{i}</large>""", table_html)

        # put the waiting, done, and running tags into ETID status table for text color
        table_html = re.sub(waiting_pattern, """<waiting>Waiting</waiting>""", table_html)
        table_html = re.sub(done_pattern, """<done>Done</done>""", table_html)
        table_html = re.sub(running_pattern, """<running>Running</running""", table_html)

        # put all html together
        full_html = html_beginning.format(title=title, description=description_text) + ip_table_html + table_html + html_ending

        # and print! :D
        print(full_html)

main()
