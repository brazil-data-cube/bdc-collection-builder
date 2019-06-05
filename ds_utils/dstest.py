from datastorm import Datastorm

host = 'http://localhost:5002'
host = 'http://www.dpi.inpe.br/opensearch/v2'

datacube = 'prodes'
datacube = 'CB4_AWFI'
datacube = 'LC8SR,S2SR,CB4_AWFI'
bands = 'nir,red'
verbose=2
w = -53.471
s = -5.66654
e = -52.482
n = -4.89771
startdate = '2017-07-01'
enddate = '2017-09-31'
type = 'MEDIAN'
type = 'SCENE'
limit = 4
verbose = 2
rp = 'SR'
d = Datastorm(host, datacube, bands, verbose)
d.search(w,s,e,n,startdate,enddate,type,rp,limit)
resolution = 0.001
d.load(resolution)
d.save()