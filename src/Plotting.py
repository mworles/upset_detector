from models.utils import convert_grid, trials_data, get_search
from models.grids import space_grid
import pandas as pd
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')

def plot_scores(df, param, log_scale = True, show = False):
    """Show line plot of cross validation scores over parameter values."""
    data_in = '../data/out/search/'
    fig = plt.figure(num=None, figsize=(8.5, 11), dpi=200, facecolor='w',
                     edgecolor='k')
    #score_type = df.loc[0, "score_type"]
    #plt.title(score_type)

    df = df.sort_values(param)

    # line plot of cross val and training scores over parameter values
    ax = plt.subplot(1, 1, 1)
    x = df[param]
    y = df['loss']
    
    plt.scatter(x, y, linestyle='--', alpha=0.7)
    plt.xlabel(param)
    
    axmin = df.describe().loc['min', param]
    axmax = df.describe().loc['max', param]
    plt.xlim(axmin, axmax)
    
    if log_scale:
        plt.xscale('log')
    else:
        pass
    plt.legend()
    #plt.ylabel(score_type)

    plt.tight_layout()

    #plot_out = '../plots/search/'
    #plot_file = plot_out + file_name + '.pdf'
    #fig.savefig(plot_file)
    #print 'saving %s' % (plot_file)

    if show == True:
        plt.show()
    else:
        plt.close('all')


trials =  get_search(0)
search_space = convert_grid(0, space_grid)
df = trials_data(trials, search_space)
print df.head()
"""
param = 'C'
df = df[df['penalty'] == 'l2']

if space_grid[0]['hyperparameters'][param]['func'] == 'loguniform':
    log_scale = True

plot_scores(df, param, log_scale = log_scale, show = True)
"""
