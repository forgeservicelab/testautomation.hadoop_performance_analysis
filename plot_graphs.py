import matplotlib
import numpy as np
import matplotlib.pyplot as plt


with open ("input.txt", "r") as myfile:
    restext = myfile.read()
    myfile.close()



def text_to_resdicts(text):
    res=[]
    entry=None
    for line in text.splitlines():
        line=line.strip()
        if not line:
            continue
        if line.startswith('#'):
            continue
        if ':' in line:
            key, value=line.split(':')
        else:
            key='name'
            value=line
            if entry:
                res.append(entry)
            entry={}

        entry[key.strip()]=value.strip()

    if entry:
        res.append(entry)
    
    return res


def dehumanize(val):
    if not type(val) in (str,unicode):
        return val
    
    # get rid of variance data in '()'
    if '(' in val:
        val=val.split('(')[0].strip()
    
    if 'MBsec' in val:
        return float(val.split(' ')[0])
    
    if 'mins' in val and 'sec' in val:
        mins, secs=val.split(',')
        mins=mins.replace('mins','').strip()
        secs=secs.replace('sec','').strip()
        return float(mins)*60 + float(secs)
    
    if 'min' in val:
        mins=val.replace('min','').strip()
        return float(mins)*60

    if 'sec' in val:
        return float(val.replace('sec','').strip())
    
    try:
        return float(val)
    except ValueError:
        pass
    
    return val


res_dicts=text_to_resdicts(restext)


for entry in res_dicts:
    for key in entry.keys():
        entry[key]=dehumanize(entry[key])


# Calculate bandwidths 
for entry in res_dicts:
    # Terasort, 500GB data, two times replication
    for key in ('gen','sort'):
        if entry.has_key(key):
            entry[key+'-bw'] = (500000*2/11) / entry[key]
    # Spark wikipedia text dump tests, 54 GB of data
    for key in ('spark-wc', 'spark-top50'):
        if entry.has_key(key):
            entry[key+'-bw'] = (54000/11) / entry[key]


def plot_bar(title, attributes, entries, colors=None):
    if not colors:
        colors=('#f1595f','#599ad3','#f9a65a','#9e66ab','#cd7058','#d77fb3','#79c36a')
    
    fig, ax = plt.subplots(figsize=(12, 6), dpi=100)
    ax.set_title(title)
    width=0.66 / len(attributes)
    i=0
    bars=[]
    for attr in attributes:
        names=[]
        values=[]
        for entry in entries:
            names.append(entry['name'])
            
            values.append(entry.get(attr, 0))

        ind = np.arange(len(values))
        bars.append(ax.bar(ind+(i*width), values, width, color=colors[i]))
        i+=1

    ax.legend(bars, attributes, loc='best')
    ax.set_xticklabels(names)
    ax.set_xticks(ind+width)
    # values on top of bars
    for bar in bars:
        for rect in bar:
            height = rect.get_height()
            ax.text(rect.get_x()+rect.get_width()/2., 1.01*height, '%.1f'%float(height),
                    ha='center', va='bottom')

if __name__ == "__main__":
#    plot_bar('Single threaded dd performance in each node (MB/sec, higher is better)',('dd-write','dd-read'), res_dicts)
#    plt.plot()
    plot_bar('Hadoop Terasort time for 500 GB dataset (seconds, lower is better)', ('gen','sort'), res_dicts)
    plt.plot()
    plot_bar('Hadoop Terasort disk bandwidth per node (MB/sec, higher is better)', ('gen-bw','sort-bw'), res_dicts)
    plt.plot()
#    plot_bar('Spark Wikipedia dump word count time (seconds, lower is better)', ('spark-wc', 'spark-top50'), res_dicts)
#    plt.plot()
#    plot_bar('Spark processing bandwidth per node (MB/sec, higher is better)', ('spark-wc-bw','spark-top50-bw'), res_dicts)
#    plt.plot()
    plt.show()

