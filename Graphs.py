import matplotlib.pyplot as plt
import matplotlib as mpl
import matplotlib.patches as patches
import numpy as np

mpl.rcParams['figure.dpi'] = 300

'''
Graphs.py

A file containing a number of functions for generating/reproducing the experimental graphs of Chord and Koorde,
along with intuitive visualizations of the DHT.
'''

def plot_Chord(pts, m, title_str='Current Chord graph'):
    '''
    Visualizing the DHT associated to the Node IDs of Chord.
    
    pts: list (int)
        List of Node IDs
    m: int
        Length of key representation in bits
    title_str:
        Title for plot
    '''
    
    MAX = 2**m
    pts.append(MAX)
    pts = np.array(pts)
    angles = -2*np.pi*((pts%MAX)/MAX) + np.pi/2
    xs, ys = np.cos(angles), np.sin(angles)
    fig, ax = plt.subplots()
    ax.set_xlim((-1.5, 1.5))
    ax.set_ylim((-1.5, 1.5))
    circle = patches.Circle((0, 0), radius=1, fill=False)
    ax.add_patch(circle)
    plt.scatter(xs[:-1], ys[:-1], c='b')
    plt.scatter(xs[-1], ys[-1], c='r')
    for i, txt in enumerate(pts):
        ax.annotate(txt, (xs[i], ys[i]))
    plt.xticks([])
    plt.yticks([])
    plt.title(title_str)
    plt.show()
    return

def whisker_plot(x, y_bot, y_diff, ylabel="", xlabel="", title="", \
                 errbarlabel = r"$5^{th}$, $95^{th}$ percentiles", \
                 centerlabel=r"$50^{th}$ percentile", \
                line=True):
    if line:
        print(np.zeros_like(y_bot).shape)
        print(y_diff.shape)
        plt.errorbar(x, y_bot, yerr=(np.zeros_like(y_bot), y_diff), capsize=1, \
                     ecolor='black', ls='', lw=1, capthick=1, \
                     label=errbarlabel, alpha=0.6)
        plt.plot(x, y_bot + (1/2)*y_diff, marker='d', \
                 color='black', linestyle='--', \
                 label=centerlabel, alpha=0.6)
    else:
        plt.errorbar(x, y_bot, yerr=(np.zeros_like(y_bot), y_diff), capsize=1, \
                     ecolor='black', ls='', lw=1, capthick=1, \
                     label=errbarlabel, alpha=0.6)
        plt.plot(x, y_bot + (1/2)*y_diff, marker='d', color='black', \
                 ls='', label=centerlabel, alpha=0.6)
    
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.show()
    return

def plot_PDF(x_values, y_values, xlabel="X-values", ylabel="PDF", title=""):
    plt.plot(x_values, pdf_values, c='black')
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.show()
    return

def whisker_plot_combined(x, y_botA, y_diffA, y_botB, y_diffB, \
                          ylabel="", xlabel="", title="", \
                 errbarlabelA = r"$5^{th}$, $95^{th}$ percentiles", \
                 centerlabelA = r"$50^{th}$ percentile", \
                errbarlabelB = r"$5^{th}$, $95^{th}$ percentiles", \
                 centerlabelB = r"$50^{th}$ percentile"):
    # Plot A
    plt.errorbar(x, y_botA, yerr=(np.zeros_like(y_botA), y_diffA), capsize=1, \
                 ecolor='blue', ls='', lw=1, capthick=1, \
                 label=errbarlabelA, alpha=0.6)
    plt.plot(x, y_botA + (1/2)*y_diffA, marker='d', \
             color='blue', linestyle='--', \
             label=centerlabelA, alpha=0.6)
    # Plot B
    plt.errorbar(x, y_botB, yerr=(np.zeros_like(y_botB), y_diffB), capsize=1, \
                 ecolor='red', ls='', lw=1, capthick=1, \
                 label=errbarlabelB, alpha=0.6)
    plt.plot(x, y_botB + (1/2)*y_diffB, marker='d', \
             color='red', linestyle='--', \
             label=centerlabelB, alpha=0.6)
    
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.show()
    return

def plot_hist(values, xlabel="X-values", ylabel="density", title=""):
    plt.hist(values, color='black', bins=np.arange(0,np.max(values)+1)+0.5)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.legend()
    plt.show()
    return