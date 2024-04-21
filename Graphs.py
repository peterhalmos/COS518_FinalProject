import matplotlib.pyplot as plt
import matplotlib.patches as patches
import numpy as np

'''
Graphs.py

A file containing a number of functions for generating/reproducing the experimental graphs of Chord and Koorde, along with intuitive visualizations of each DHT.
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
    