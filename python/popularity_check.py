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

print 'Power law alpha: %f' % fit.power_law.alpha
print 'Power law D: %f' % fit.power_law.D
print 'Power law xmin: %d' % fit.power_law.xmin

fit.lognormal.plot_ccdf(ax=fig, color='g', linestyle='-', label='Log Normal')
print 'Log normal mu: %f' % fit.lognormal.mu
print 'Log normal sigma: %f' % fit.lognormal.sigma

R, p = fit.distribution_compare('power_law', 'lognormal', normalized_ratio=True)
print 'R %f, p %f' % (R,p)

plt.legend()
plt.show()
