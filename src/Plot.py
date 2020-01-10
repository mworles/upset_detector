from models.utils import convert_grid, trials_data, get_search
from models.grids import space_grid
import pandas as pd
from sklearn.metrics import roc_curve, confusion_matrix
import matplotlib.pyplot as plt
plt.style.use('fivethirtyeight')
import seaborn as sns


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

def plot_roc_curve(y, y_prob, grid_id, path='../plots/'):
    fig = plt.figure(figsize=(8, 6))
    fpr, tpr, thresh = roc_curve(y, y_prob)
    plt.plot(fpr, tpr, lw=3) #, label=s['model'])
    plt.ylim([0.0, 1.0])
    plt.xlim([0.0, 1.0])
    title = "ROC Curve" #.join([label_name, " Classifier ROC Curve"])
    plt.title(title, fontsize=20)
    plt.xlabel('False Positive Rate', fontsize=18)
    plt.ylabel('True Positive Rate', fontsize=18)
    plt.yticks(fontsize=16)
    plt.xticks(fontsize=16)
    plt.tight_layout()
    #plt.legend(frameon=True, fontsize=14, loc=0, facecolor='white')
    f = "_".join(['grid', str(grid_id), 'roc.jpg'])
    plt.savefig(path + f)

def plot_confusion_matrix(y, y_pred, grid_id, path='../plots/'):
    fig = plt.figure(figsize=(8, 4))
    cm = confusion_matrix(y, y_pred)
    df_cm = pd.DataFrame(cm, index = ['Neg', 'Pos'], columns = ['Neg', 'Pos'])
    fig, ax = plt.subplots(figsize = (4, 4))
    ax = plt.axes()
    sns.heatmap(df_cm, annot=True, fmt=".0f", cmap='Blues') #, annot_kws={"size": 18})
    sns.set(font_scale=2.0)
    ax.xaxis.tick_top()
    ax.xaxis.set_label_position('top')
    #title = "".join([label_name, " Matrix"])
    #plt.title(title, fontsize=20)
    ax.set_ylabel('True')
    ax.set_xlabel('Prediction')
    plt.tight_layout()
    f = "_".join(['grid', str(grid_id), 'cnfmat.jpg'])
    plt.savefig(path + f)
