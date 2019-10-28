import sys
sys.path.append("../")
import pandas as pd
from Cleaning import write_file

print 'running %s' % (os.path.basename(__file__))

# read in all separate feature data files
dir = '../../data/'
ts = pd.read_csv(dir + '/raw/NCAATourneySeeds.csv')
