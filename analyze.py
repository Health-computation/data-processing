import pandas as pd
import datetime
from optparse import OptionParser
import sys
import dbmerge
import db2csv
import os
import glob

def get_phone_usage_duration(ts):
    """
    Get phone usage duration in seconds.
    
    :param ts: Screen usage time series sorted in ascending order.
    
    :returns:: A `DataFrame` with screen usage start, end time.
               Duration of usage and non-usage is calculated as well.
               For these calculations, time of end-usage is the anchoring
               point --- subtracting previous start for usage and subtracting
               from next start for non-usage.
               
    """
    usage_start = []
    usage_end = []
    if not ts[0]:
        ts = ts[1:]
    x = 0
    while x < len(ts):
        # If the current event is screen-on and
        # next one is screen-off, then the duration
        # of phone usage can be determined by subtracting.
        if ts[x] and not ts[x + 1]:
            on = ts.index[x]
            off = ts.index[x + 1]
            x += 2
        else:
            # This is not the pair of events we are looking for.
            x += 1
            continue
        # Screen on time stamp
        usage_start.append(on)
        usage_end.append(off)
        
    duration = pd.DataFrame({"begins": usage_start, "ends": usage_end})
    duration['duration'] = duration.ends - duration.begins
    duration['n_duration'] = duration.begins.shift(-1) - duration.ends
    # duration = duration.set_index(duration.u_start)
    return duration

def get_sleep_info(df, duration_threshold=60):
    """
    Gets sleep duration for each day.
    
    The sleep duration is indicated by the non-usage duration. In determining
    non-usage duration, any phone usage duration less than `duration_threshold`
    is ignored.
    
    Sleep duration is associated with the day of sleep onset --- with mid-day
    is the anchor point.
    
    There are a number of possible heuristics we can use:
    
        1. If the sleep onset is during night (Similar to MCTQ for non-shift workers).
        2. Limit on duration of sleep (<= 12 hours).
        3. Combining more than one consecutive non-usage
        
    :param df: Dataframe with `begins`, `ends`, `duration` (`ends - begins`) of 
               phone usage.
               
    :return:: Returns a DataFrame with index as the day and sleep `begins`, `ends` and
              `duration`.
    """
    
    filtered_df = df[df.duration >= np.timedelta64(duration_threshold, 's')].copy()
    filtered_df['sleep_duration'] = filtered_df.begins.shift(-1) - filtered_df.ends
    
    # Mid-day is the anchoring point
    g = filtered_df.groupby(lambda x: x.date() - datetime.timedelta(days=1) if x.hour <= 12 else x.date())
    
    days = []
    sleep_onset = []
    sleep_duration = []
    for k, v in g:

        filtering = v.apply(lambda x: x['ends'].hour in [22, 23, 0, 1, 2, 3, 4, 5, 6, 7], axis=1)
        v = v[filtering]
        sleep = v.sort(['sleep_duration'], ascending=False)[:1]
        # We are using index to keep it timezone naive timestamp.
        # sleep.ends internall keep the value as np.datetime64 which
        # assumes local timezone when none is specified.
        if len(sleep) > 0:
            days.append(k)
            sleep_onset.append(sleep.index[0])
            sleep_duration.append(sleep.sleep_duration.values[0])
        
    sleep_df = pd.DataFrame({
                             "sleep_begins": sleep_onset,
                             "sleep_duration": sleep_duration
                             }, index=days)
    # The duration type is coerced to np.timedelta64 as internally it
    # is just an 
    sleep_df['sleep_duration']  = sleep_df.sleep_duration.astype(np.timedelta64)
    sleep_df.index = pd.to_datetime(sleep_df.index)
    
    return sleep_df

def run_analysis(screen_probe_csv_file, output_file):
    screen_probe = pd.read_csv(screen_probe_csv_file, usecols=['timestamp', 'screenOn'],
                parse_dates=['timestamp'], date_parser=lambda x: datetime.datetime.fromtimestamp(float(x)).replace(tzinfo=None))
    screen_probe = screen_probe.set_index(screen_probe.timestamp)
    screen_probe = screen_probe.sort_index()
    screen_probe.to_csv(output_file)

if __name__ == '__main__':
    parser = OptionParser("usage: %prog arg1 arg2")
    (options, args) = parser.parse_args()
    try:
        os.chdir("/var/uploads/test_data")
        # Merges all of the files in test data, overwriting the old merged.db file
        dbmerge.merge(None, "/var/uploads/processed/merged.db", True)

        print "Merge completed"
        # Convert merged.db file to a csv file that can be processed and placed in the processed folder
        for file in glob.glob("/var/uploads/processed/*.db"):
            db2csv.convert(file, "/var/uploads/processed/done")
        print "db2csv completed"
        print args[0]
        print args[1]
        run_analysis(args[0], args[1])
    except Exception as e:
        sys.exit("ERROR: " + str(e))
