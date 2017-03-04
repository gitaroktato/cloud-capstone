import powerlaw
import matplotlib.pyplot as plt

file = open('./popular_distrib.log', 'r')
data = []
for line in file:
	elements = line.split()
	data.append(float(elements[0]))

fit = powerlaw.Fit(data)
fig = fit.plot_ccdf(color='b', label='Empirical Data')
fit.power_law.plot_ccdf(ax=fig, color='r', linestyle='-', label='Power Law')
fit.lognormal.plot_ccdf(ax=fig, color='g', linestyle='-', label='Log Normal')
plt.legend()
plt.show()
