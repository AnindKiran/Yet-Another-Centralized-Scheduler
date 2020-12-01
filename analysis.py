import statistics
import matplotlib.pyplot as plt
import numpy as np

j_rr=dict()
t_rr=dict()
w_rr=dict()
j_l=dict()
t_l=dict()
w_l=dict()
j_ra=dict()
t_ra=dict()
w_ra=dict()

j=dict()
t=dict()

# reading log file and creating dictionaries to store job, task and worker data
with open(r"C:\Users\Nikitha\Documents\NIKITHA\Sem 5\BD\YACS\log.txt","r") as logfile:
     lines = (line for line in (l.strip() for l in logfile) if line)
     for line in lines:
        algo=''
        if(line):
            if(line[0] == '*'):
                algo = line[3:5]
                if(algo == 'RR' or algo == 'rr'):
                    j = j_rr
                    t = t_rr
                    w = w_rr
                elif(algo == 'LL' or algo == 'll'):
                    j = j_l
                    t = t_l
                    w = w_l
                else:
                    j = j_ra
                    t = t_ra
                    w = w_ra
            else:
                vals = list(line.split())
                code = vals[0]
                h,m,s = (vals[1].split(':'))
                no = vals[3]
                h = int(h)
                m = int(m)
                s = int(s)
                if(len(vals) > 4):
                    if(vals[4] not in w.keys()):
                        w[vals[4]] = []
                    w[vals[4]].append(h*3600+m*60+s)
                if(code=='00'):
                    j[no] = (h*3600+m*60+s)
                if(code=='11'):
                    j[no] = (h*3600+m*60+s) - j[no]
                if(code=='01'):
                    t[no] = (h*3600+m*60+s)
                if(code=='10'):
                    t[no] = (h*3600+m*60) + s-t[no]

# calculating mean and median time taken for tasks and jobs, algorithm wise
task_mean_rr = statistics.mean(list(t_rr.values()))
job_mean_rr = statistics.mean(list(j_rr.values()))
task_mean_l = statistics.mean(list(t_l.values()))
job_mean_l = statistics.mean(list(j_l.values()))
task_mean_ra = statistics.mean(list(t_ra.values()))
job_mean_ra = statistics.mean(list(j_ra.values()))

task_median_rr = statistics.median(list(t_rr.values()))
job_median_rr = statistics.median(list(j_rr.values()))
task_median_l = statistics.median(list(t_l.values()))
job_median_l = statistics.median(list(j_l.values()))
task_median_ra = statistics.median(list(t_ra.values()))
job_median_ra = statistics.median(list(j_ra.values()))

print()
print("ROUND ROBIN")
print("mean-task:",task_mean_rr)
print("median-task:",task_median_rr)
print("mean-job:",job_mean_rr)
print("median-job:",job_median_rr)
print("---------------------------------")

print()
print("LEAST LOADED")
print("mean-task:",task_mean_l)
print("median-task:",task_median_l)
print("mean-job:",job_mean_l)
print("median-job:",task_median_l)
print("---------------------------------")

print()
print("RANDOM")
print("mean-task:",task_mean_ra)
print("median-task:",task_median_ra)
print("mean-job:",job_mean_ra)
print("median-job:",task_mean_ra)
print("---------------------------------")
print()

# graphs for mean and median time taken by jobs and tasks algorithm wise, plotted for each algorithm separately
# for round robin 
barWidth = 0.25
bars1 = [task_mean_rr, job_mean_rr]
bars2 = [task_median_rr, job_median_rr]
r1 = np.arange(len(bars1))
r2 = [x + barWidth for x in r1]
plt.bar(r1, bars1, color='red', width=barWidth, edgecolor='black', label='Mean')
plt.bar(r2, bars2, color='blue', width=barWidth, edgecolor='black', label='Median')
plt.xticks([r + barWidth/2 for r in range(len(bars1))], ['Tasks', 'Jobs'])
plt.title("ROUND ROBIN") 
plt.legend()
plt.show()

# for least loaded
bars1 = [task_mean_l, job_mean_l]
bars2 = [task_median_l, job_median_l]
r1 = np.arange(len(bars1))
r2 = [x + barWidth for x in r1]
plt.bar(r1, bars1, color='red', width=barWidth, edgecolor='black', label='Mean')
plt.bar(r2, bars2, color='blue', width=barWidth, edgecolor='black', label='Median')
plt.xticks([r + barWidth/2 for r in range(len(bars1))], ['Tasks', 'Jobs'])
plt.title("LEAST LOADED") 
plt.legend()
plt.show()

# for random
bars1 = [task_mean_ra, job_mean_ra]
bars2 = [task_median_ra, job_median_ra]
r1 = np.arange(len(bars1))
r2 = [x + barWidth for x in r1]
plt.bar(r1, bars1, color='red', width=barWidth, edgecolor='black', label='Mean')
plt.bar(r2, bars2, color='blue', width=barWidth, edgecolor='black', label='Median')
plt.xticks([r + barWidth/2 for r in range(len(bars1))], ['Tasks', 'Jobs'])
plt.title("RANDOM") 
plt.legend()
plt.show()


# graphs for mean and median time taken by jobs and tasks algorithm wise, plotted for tasks and jobs separately
# for tasks
barWidth = 0.25
bars1 = [task_mean_rr, task_mean_l, task_mean_ra]
bars2 = [task_median_rr, task_median_l, task_median_ra]
r1 = np.arange(len(bars1))
r2 = [x + barWidth for x in r1]
plt.bar(r1, bars1, color='red', width=barWidth, edgecolor='black', label='Mean')
plt.bar(r2, bars2, color='blue', width=barWidth, edgecolor='black', label='Median')
plt.xticks([r + barWidth/2 for r in range(len(bars1))], ['Round Robin', 'Least Loaded', 'Random'])
plt.title("Mean and median of tasks") 
plt.legend()
plt.show()

# for jobs
bars1 = [job_mean_rr, job_mean_l, job_mean_ra]
bars2 = [job_median_rr, job_median_l, job_median_ra]
r1 = np.arange(len(bars1))
r2 = [x + barWidth for x in r1]
plt.bar(r1, bars1, color='red', width=barWidth, edgecolor='black', label='Mean')
plt.bar(r2, bars2, color='blue', width=barWidth, edgecolor='black', label='Median')
plt.xticks([r + barWidth/2 for r in range(len(bars1))], ['Round Robin', 'Least Loaded', 'Random'])
plt.title("Mean and median of jobs") 
plt.legend()
plt.show()



# plotting scatter plots for the time at which tasks were scheduled at each worker, algorithm wise
# one plot for each algorithm
w = []
t = []
for i in w_rr.keys():
    for j in w_rr[i]:
        w.append(i)
        t.append(j)
plt.scatter(w,t)
plt.title('tasks scheduled on workers Vs time of scheduling - ROUND ROBIN ')
plt.xlabel('workers')
plt.ylabel('time')
plt.show()

w = []
t = []
for i in w_l.keys():
    for j in w_l[i]:
        w.append(i)
        t.append(j)
plt.scatter(w,t)
plt.title('tasks scheduled on workers Vs time of scheduling - LEAST LOADED ')
plt.xlabel('workers')
plt.ylabel('time')
plt.show()

w = []
t = []
for i in w_ra.keys():
    for j in w_ra[i]:
        w.append(i)
        t.append(j)
plt.scatter(w,t)
plt.title('tasks scheduled on workers Vs time of scheduling - RANDOM ')
plt.xlabel('workers')
plt.ylabel('time')
plt.show()


