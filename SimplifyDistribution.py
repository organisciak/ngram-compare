import pandas as pd
import numpy as np
import argparse
import sys

parser = argparse.ArgumentParser(description="Simplify an NGrams file from Google.")
parser.add_argument('inpath', type=str,
        help='The file to progress.')
parser.add_argument('outfile', nargs='?', type=argparse.FileType('w'),
        default=sys.stdout, help='The CSV file to write to.')
args = parser.parse_args()

# Since the data is big and we'll be simplify it, let's read in chunks and 
# fold chunk by chunk

chunksize = 20000
reader = pd.read_csv(args.inpath, 
                   sep='\t', compression='infer', 
                   usecols = ['term', 'year', 'match_count'],
                   chunksize=chunksize,
                   #nrows = 50000, #FOR TESTING
                   names=['term', 'year', 'match_count', 'volume_count'])

data  = None
aside = None

i = 0
print "reading chunk:",
for chunk in reader:
    print "chunk %d of %s," % (i, args.inpath)
    # Trim to better represented dates
    chunk = chunk.query('year >= 1810')
    
    # If we took rows for a term off from the last chunk, append it now
    if type(aside) is pd.core.frame.DataFrame:
        chunk = pd.concat([aside, chunk])
    
    # Find the where the last term started. Since it is likely split by the cut, we'll remove it
    # and tack it on to the start of the next batch
    if len(chunk) == chunksize:
        end = len(chunk)-1
        word = chunk.iloc[end]['term']
        # Working backwards
        while True:
            end -= 1
            nextword = chunk.iloc[end]['term']
            if word != nextword:
                end += 1
                break
        
        aside = chunk.iloc[end:]
        chunk = chunk.iloc[:end]
    
    # Replace Year with decade
    chunk['decade'] = chunk['year'].apply(lambda x: 10 * np.floor_divide(x, 10))
    chunk.drop('year', axis=1, inplace=True)
    # TODO pull out and set aside any terms that were cut off
    
    # Remove any word where there isn't a sum of 1000 matches throughout everything
    chunk = chunk.groupby(['term']).filter(lambda x: x['match_count'].sum() > 1000).reset_index()
    
    # Sum by decade
    chunk = chunk.groupby(['term','decade']).sum().reset_index()
    
    # Pivot to wide format
    chunk = chunk.pivot(index='term', columns='decade', values='match_count')            .fillna(0)
    
    if i is 0:
        data = chunk
    else:
        data = pd.concat([ data, chunk ])
    i += 1
    
    # For testing
    #if i >=5:
    #    break


data.to_csv(args.outfile)
